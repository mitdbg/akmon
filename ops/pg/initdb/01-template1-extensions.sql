-- Install the substrate extensions in `template1` as well, so every
-- database later created on this instance inherits them.
--
-- Why: tests build their own scratch databases with a bare
-- `CREATE DATABASE`, which copies `template1`. Without this file those
-- databases have only `plpgsql`, and the first BM25 index the migration
-- creates fails with `access method "bm25" does not exist`.
--
-- `pg_cron` is deliberately absent. It can be created in exactly one
-- database per instance -- the one named by `cron.database_name`, which
-- this image sets to `okg` -- and `CREATE EXTENSION pg_cron` in any other
-- database errors with `can only create extension in database okg`.
-- Putting it here would abort initialization. Code that needs cron uses
-- the `okg` database; the test fixtures already drop `pg_cron` from their
-- required set for the same reason.
--
-- Runs after 00-extensions.sql (lexical order). The `\c` below switches
-- this psql session to `template1` for the rest of the file.
--
-- Expect a `trying to connect to template database, exiting` error from the
-- TimescaleDB background-worker launcher during initdb: the launcher is
-- already running when the extension appears, starts a scheduler for
-- template1, and that scheduler exits. That one is this file's doing. A
-- second such line has been observed about 90 seconds after the server
-- starts, with none after that including after a container restart; that one
-- is NOT caused by this file -- it appears on an image whose template1 is
-- bare too. The post-start line is host-dependent; ops/pg/README.md records
-- both counts.
--
-- This also puts a TimescaleDB scheduler and two scheduled jobs (telemetry
-- and job-history retention) in every database cloned from template1, and
-- `okg migrate` adds 22 retention and compression policies, so a migrated okg
-- database schedules 24 jobs, every one with max_retries = -1. Each scheduler
-- and each running job takes a max_worker_processes slot. With PostgreSQL's
-- default of 8 slots, five migrated okg databases left no slot for any job:
-- every launch failed with `failed to start a background worker`, was retried
-- without limit, and wrote a row to
-- _timescaledb_internal.bgw_job_stat_history, until the disk filled (issue
-- #3143). These failures are NOT harmless. The image and the compose files
-- now set max_worker_processes = 32; ops/pg/README.md has the arithmetic and
-- the measurement. TimescaleDB also caps its own schedulers and jobs at
-- timescaledb.max_background_workers (16), so about 15 or more databases on
-- one instance starve their jobs whatever the slot count. `okg deployment
-- ready` warns about both (check database.timescaledb_workers).
\c template1

CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_textsearch;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;
CREATE EXTENSION IF NOT EXISTS btree_gist;

DO $$
DECLARE r RECORD;
BEGIN
  RAISE NOTICE 'Extensions installed in template1:';
  FOR r IN SELECT extname, extversion FROM pg_extension ORDER BY extname LOOP
    RAISE NOTICE '  %  %', r.extname, r.extversion;
  END LOOP;
END
$$;
