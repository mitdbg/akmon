SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

-- PACT bound-okg-workspace-disk-growth, req.bound-disk.intervals-sidecar.
-- presence-means-overlay: the effective-attribute view carried raw_attrs AND
-- attrs per row even when no overlay applied (measured 100% of a 200k sample
-- byte-identical) -- a pure 2x jsonb duplication, and the interval sidecar is
-- the DB's #2 consumer (34 GB). Relax raw_attrs from NOT NULL DEFAULT '{}' to a
-- nullable column with no default: NULL now means "raw == attrs" and readers
-- fall back via COALESCE(raw_attrs, attrs). The bounded-batch backfill that NULLs
-- EXISTING duplicate rows is a separate operator step (okg db
-- backfill-effective-interval-raw-attrs), NOT an unbounded UPDATE here.
--
-- Idempotent: DROP NOT NULL on an already-nullable column and DROP DEFAULT on a
-- column with no default are both no-ops. The DO block tolerates a deployment
-- whose Python-owned schema has not created these tables yet AND (post
-- schema-interval-narrow-split, migration 0020) a fresh database whose tables
-- were born WITHOUT the raw_attrs column at all -- the payloads live in
-- okg.attribute_versions there, so this relaxation has nothing to do.
-- (Audited edit: the previous ALTER TABLE IF EXISTS form errored on the
-- narrow, raw_attrs-less shape; durable DDL on the wide shape is identical.
-- The legacy checksum is registered in
-- management._ORDERED_MIGRATION_CHECKSUM_COMPATIBILITIES.)

DO $raw_attrs_nullable$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_schema = 'okg' AND table_name = 'attribute_effective_view'
       AND column_name = 'raw_attrs'
  ) THEN
    ALTER TABLE okg.attribute_effective_view
      ALTER COLUMN raw_attrs DROP NOT NULL;
    ALTER TABLE okg.attribute_effective_view
      ALTER COLUMN raw_attrs DROP DEFAULT;
  END IF;
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_schema = 'okg'
       AND table_name = 'attribute_effective_view_intervals'
       AND column_name = 'raw_attrs'
  ) THEN
    ALTER TABLE okg.attribute_effective_view_intervals
      ALTER COLUMN raw_attrs DROP NOT NULL;
    ALTER TABLE okg.attribute_effective_view_intervals
      ALTER COLUMN raw_attrs DROP DEFAULT;
  END IF;
END
$raw_attrs_nullable$;
