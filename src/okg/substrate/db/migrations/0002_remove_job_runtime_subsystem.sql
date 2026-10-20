-- Remove both obsolete generations of the OKG job-runtime subsystem.
--
-- The caller must first stop/drain old runtime workers, replace the legacy
-- lock views with the current canonical definitions, and verify that neither
-- DBOS nor the legacy ledger has active work. Restrictive drops intentionally
-- fail if an unknown dependent object still exists.

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

DROP VIEW IF EXISTS okg.v_job_runtime_reconciliation;
DROP VIEW IF EXISTS okg.v_job_queue;
DROP FUNCTION IF EXISTS okg.job_runtime_reconciliation();

DO $$
DECLARE
  legacy_job record;
BEGIN
  IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_cron') THEN
    FOR legacy_job IN
      SELECT jobid
        FROM cron.job
       WHERE jobname IN ('okg-publish', 'okg-lease-sweep')
         AND database = current_database()
    LOOP
      PERFORM cron.unschedule(legacy_job.jobid);
    END LOOP;
  END IF;
END
$$;

DROP TABLE IF EXISTS okg.job_external_calls;
DROP TABLE IF EXISTS okg.job_item_frontiers;
DROP TABLE IF EXISTS okg.job_items;
DROP TABLE IF EXISTS okg.job_chunks;
DROP TABLE IF EXISTS okg.job_runs;
DROP TABLE IF EXISTS okg.job_dependencies;
DROP TABLE IF EXISTS okg.job_definitions;
DROP TABLE IF EXISTS okg.job_runtime_ledger_retirement_marker;
DROP INDEX IF EXISTS okg.node_facts_workload_source_fact_idx;
DROP INDEX IF EXISTS okg.edge_facts_workload_source_fact_idx;
DROP INDEX IF EXISTS okg.edge_facts_source_pattern_idx;
DROP INDEX IF EXISTS okg.jobs_live_dedupe_uidx;
DROP TABLE IF EXISTS okg.jobs;
DROP TABLE IF EXISTS okg.publisher_leases;
DROP TABLE IF EXISTS okg.legacy_runtime_retirement_marker;
