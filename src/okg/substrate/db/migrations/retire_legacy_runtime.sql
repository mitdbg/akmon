-- Phase 4: retire the legacy runtime layer.
--
-- Per `migrate-substrate-runtime-to-dbos` §4. Drops:
--   * okg.jobs — generic job queue (job_kind: ingest, link, enrich,
--                publish, embed).
--   * okg.publisher_leases — TTL-based publisher lease.
--   * okg.jobs_live_dedupe_uidx — partial unique index supporting
--                                  okg.jobs's "at most one live copy"
--                                  contract.
--   * pg_cron entries: okg-publish, okg-lease-sweep — substrate-owned
--                      cron schedules.
--
-- Pre-condition: an operator confirms there is no pending legacy
-- work. The script aborts before any destructive operation if it
-- finds rows in okg.jobs with status IN ('queued', 'leased') — that
-- indicates a live workflow against the legacy queue and dropping
-- it would lose work.
--
-- Reversibility: this migration is **destructive**. The dropped
-- tables and indexes are not recreatable from the substrate's
-- schema.sql (the legacy schema is being retired). Operators who
-- need historical job records for audit MUST dump the table to cold
-- storage before running this migration.
--
-- The migration writes a (job_kind, status, count, min_created_at,
-- max_created_at) aggregate to stdout AND inserts the same aggregate
-- into okg.deployment_audit_log (when present) so an operator can
-- reconstruct historical job-counts after the table is gone.

-- =====================================================================
-- 0. Installed-schema preflight + safety check.
-- =====================================================================

DO $$
DECLARE
  pending_count bigint;
  detail text;
  missing_columns text[];
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = 'okg' AND c.relname = 'jobs'
  ) THEN
    RAISE NOTICE 'okg.jobs not present; migration is a no-op.';
    RETURN;
  END IF;

  SELECT array_agg(required.column_name ORDER BY required.column_name)
    INTO missing_columns
    FROM (
      VALUES
        ('job_id'),
        ('job_kind'),
        ('status'),
        ('dedupe_key'),
        ('created_at')
    ) AS required(column_name)
    WHERE NOT EXISTS (
      SELECT 1
        FROM information_schema.columns c
       WHERE c.table_schema = 'okg'
         AND c.table_name = 'jobs'
         AND c.column_name = required.column_name
    );

  IF missing_columns IS NOT NULL THEN
    RAISE EXCEPTION
      'cannot retire legacy runtime: okg.jobs is missing required column(s): %',
      missing_columns
      USING ERRCODE = 'check_violation';
  END IF;

  SELECT count(*) INTO pending_count
    FROM okg.jobs
   WHERE status IN ('queued', 'leased');

  IF pending_count > 0 THEN
    SELECT string_agg(
             format('  job_id=%s kind=%s status=%s dedupe_key=%s',
                    job_id, job_kind, status, dedupe_key),
             E'\n')
      INTO detail
      FROM (
        SELECT job_id, job_kind, status, dedupe_key
          FROM okg.jobs
         WHERE status IN ('queued', 'leased')
         ORDER BY job_id
         LIMIT 10
      ) s;
    RAISE EXCEPTION
      E'cannot retire legacy runtime: % rows in okg.jobs have status in (queued, leased). Drain or cancel them first.\nFirst 10 offenders:\n%',
      pending_count, COALESCE(detail, '<none>')
      USING ERRCODE = 'check_violation';
  END IF;
END
$$;


-- =====================================================================
-- 1. Archive aggregate: write a one-time summary so the operator
--    has a record after the table is dropped. We write to
--    okg.deployment_audit_log if it exists; otherwise stdout.
-- =====================================================================

DO $$
DECLARE
  has_audit_log boolean;
  agg_row record;
  summary_json jsonb := '[]'::jsonb;
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = 'okg' AND c.relname = 'jobs'
  ) THEN
    RETURN;
  END IF;

  -- Build aggregate.
  FOR agg_row IN
    SELECT job_kind,
           status,
           count(*) AS row_count,
           min(created_at) AS min_created_at,
           max(created_at) AS max_created_at
      FROM okg.jobs
     GROUP BY job_kind, status
     ORDER BY job_kind, status
  LOOP
    summary_json := summary_json || jsonb_build_object(
      'job_kind', agg_row.job_kind,
      'status', agg_row.status,
      'row_count', agg_row.row_count,
      'min_created_at', agg_row.min_created_at,
      'max_created_at', agg_row.max_created_at
    );
    RAISE NOTICE 'retire_legacy_runtime archive: job_kind=% status=% count=% min_created_at=% max_created_at=%',
      agg_row.job_kind, agg_row.status, agg_row.row_count,
      agg_row.min_created_at, agg_row.max_created_at;
  END LOOP;

  SELECT EXISTS (
    SELECT 1 FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = 'okg' AND c.relname = 'deployment_audit_log'
  ) INTO has_audit_log;

  IF has_audit_log THEN
    EXECUTE
      'INSERT INTO okg.deployment_audit_log '
      '  (event_kind, payload, observed_at) '
      'VALUES ($1, $2, now())'
      USING 'legacy_runtime_retired', summary_json;
  END IF;
END
$$;


-- =====================================================================
-- 2. Drop pg_cron entries (idempotent).
-- =====================================================================

DO $$
DECLARE
  cron_present boolean;
BEGIN
  SELECT EXISTS (
    SELECT 1 FROM pg_extension WHERE extname = 'pg_cron'
  ) INTO cron_present;
  IF NOT cron_present THEN
    RAISE NOTICE 'pg_cron not installed; skipping cron unschedule.';
    RETURN;
  END IF;

  -- cron.unschedule(name) raises if the name doesn't exist; guard.
  IF EXISTS (
    SELECT 1 FROM cron.job WHERE jobname = 'okg-publish'
  ) THEN
    PERFORM cron.unschedule('okg-publish');
    RAISE NOTICE 'unscheduled cron: okg-publish';
  END IF;
  IF EXISTS (
    SELECT 1 FROM cron.job WHERE jobname = 'okg-lease-sweep'
  ) THEN
    PERFORM cron.unschedule('okg-lease-sweep');
    RAISE NOTICE 'unscheduled cron: okg-lease-sweep';
  END IF;
END
$$;


-- =====================================================================
-- 3. Drop the legacy tables + indexes.
-- =====================================================================

DROP INDEX IF EXISTS okg.jobs_live_dedupe_uidx;
DROP TABLE IF EXISTS okg.jobs CASCADE;
DROP TABLE IF EXISTS okg.publisher_leases CASCADE;


-- =====================================================================
-- 4. Marker: legacy runtime retired for this deployment.
-- =====================================================================

CREATE TABLE IF NOT EXISTS okg.legacy_runtime_retirement_marker (
  retired_at timestamptz NOT NULL DEFAULT now(),
  proposal text NOT NULL DEFAULT 'migrate-substrate-runtime-to-dbos'
);

INSERT INTO okg.legacy_runtime_retirement_marker (retired_at)
SELECT now()
WHERE NOT EXISTS (
  SELECT 1 FROM okg.legacy_runtime_retirement_marker
);
