SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

-- PACT schema-namespace-qualification, req.schema-namespace-qualification.
-- pact-tables-migrated.
--
-- The three PACT ledger relations were created unqualified by the Python
-- ledger path, so search_path resolved them into `public` while every other
-- application relation lives in `okg`. The namespace contract (D4) admits
-- exactly one family in `public` -- the vendored `_chronos*` physicals -- so
-- these three move.
--
-- ALTER TABLE ... SET SCHEMA is a catalog update: it rewrites no heap pages
-- and copies no rows, so this is instant and lossless even on a populated
-- ledger. Owned indexes, constraints, and the identity sequences move with
-- their table.
--
-- Ordering with the snapshot: the ordered runner executes AFTER the schema
-- body, and the snapshot creates these tables only when no `public`
-- predecessor is present (see the `pact_ledger_tables` block in schema.sql).
-- That guard is what leaves the name free for this move. Both relations
-- existing at once means something outside this path created one; that is
-- ambiguous -- which copy is the ledger? -- so it refuses rather than guess.
--
-- Idempotent: on a fresh install, and on any database that already ran this,
-- to_regclass on the `public` name is NULL and every branch is skipped.

DO $pact_ledger_schema_qualification$
DECLARE
    relname TEXT;
BEGIN
    FOREACH relname IN ARRAY ARRAY[
        'pact_decisions', 'pact_intents', 'pact_coordination_events'
    ] LOOP
        IF to_regclass('public.' || relname) IS NULL THEN
            CONTINUE;
        END IF;

        IF to_regclass('okg.' || relname) IS NOT NULL THEN
            RAISE EXCEPTION
                'pact ledger % exists in BOTH public and okg; refusing to '
                'guess which one holds the recorded decisions. Reconcile by '
                'hand: keep the populated relation, drop the empty one, then '
                're-run migrate.', relname
                USING ERRCODE = 'duplicate_table';
        END IF;

        EXECUTE format('ALTER TABLE public.%I SET SCHEMA okg', relname);
        RAISE NOTICE 'pact ledger %: public -> okg', relname;
    END LOOP;
END
$pact_ledger_schema_qualification$;
