-- Create extensions at first boot. Safe to re-run on idempotent retry.
-- Order matters: timescaledb first, then the others.
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS vector;
-- pg_cron only on a server that preloads it. CREATE EXTENSION pg_cron fails
-- unless pg_cron is in shared_preload_libraries (it errors with
-- `unrecognized configuration parameter "cron.database_name"`), and substrate
-- pg_cron schedules are retired (DBOS owns runtime schedules, commit
-- 627f805dd0), so the okg-workspace dogfood server no longer preloads it.
-- ops/pg/docker-compose.yaml still preloads it with cron.database_name=okg and
-- still gets the extension. A server that preloads pg_cron but names a
-- different cron.database_name still fails here, loudly, as before.
--
-- The check reads each comma-separated entry the way Postgres accepts it:
-- surrounding whitespace (spaces, tabs, newlines) and double quotes trimmed,
-- any directory such as `$libdir/` dropped, and a `.so` suffix dropped, so
-- `$libdir/pg_cron.so` counts and `my_pg_cron` does not.
-- tests/substrate/test_initdb_pg_cron_guard_pg.py runs this IF condition
-- against a real server; keep it a single boolean expression ending in THEN
-- directly before the CREATE.
DO $$
BEGIN
  IF EXISTS (
    SELECT 1
      FROM regexp_split_to_table(current_setting('shared_preload_libraries'), ',') AS entry
     WHERE regexp_replace(
             regexp_replace(
               regexp_replace(entry, '^[[:space:]"]+|[[:space:]"]+$', '', 'g'),
               '^.*/', ''
             ),
             '[.]so$', ''
           ) = 'pg_cron'
  ) THEN
    CREATE EXTENSION IF NOT EXISTS pg_cron;
  ELSE
    RAISE NOTICE 'pg_cron is not in shared_preload_libraries; not creating it';
  END IF;
END
$$;
CREATE EXTENSION IF NOT EXISTS pg_textsearch;
-- Trigram similarity (fuzzy alias resolution) + edit-distance /
-- phonetic match, plus GiST equality operator classes for
-- bitemporal live-table exclusion constraints. All are stock
-- Postgres contrib modules.
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;
CREATE EXTENSION IF NOT EXISTS btree_gist;

-- Roles that `okg migrate` will also create, created here WITHOUT login.
-- Earlier images gave them LOGIN with fixed passwords committed in source, so
-- any account that could reach the server could log in as them (issue #3273).
-- After `okg migrate --apply`, run `okg db role-passwords --apply`: it gives
-- okg_mcp and app_rw LOGIN with random passwords and records them in an
-- owner-only file that `okg mcp-serve` reads. A single-user development
-- machine can opt back into the public passwords with
-- OKG_ALLOW_BUILTIN_ROLE_PASSWORDS=1 and `--builtin-dev-passwords`.
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_rw') THEN
    CREATE ROLE app_rw NOLOGIN;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_ro') THEN
    CREATE ROLE app_ro NOLOGIN;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'okg_mcp') THEN
    CREATE ROLE okg_mcp NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'catalog_admin') THEN
    CREATE ROLE catalog_admin NOLOGIN;
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_roles WHERE rolname = 'projection_remediator'
  ) THEN
    CREATE ROLE projection_remediator NOLOGIN;
  END IF;
END
$$;

-- Sanity: print versions for humans tailing the log.
DO $$
DECLARE r RECORD;
BEGIN
  RAISE NOTICE 'Installed extensions:';
  FOR r IN SELECT extname, extversion FROM pg_extension ORDER BY extname LOOP
    RAISE NOTICE '  %  %', r.extname, r.extversion;
  END LOOP;
END
$$;
