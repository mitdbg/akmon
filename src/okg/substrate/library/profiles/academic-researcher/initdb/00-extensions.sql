-- Create extensions at first boot. Safe to re-run on idempotent retry.
-- Order matters: timescaledb first, then the others.
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_textsearch;
-- Trigram similarity (fuzzy alias resolution) + edit-distance /
-- phonetic match, plus GiST equality operator classes for
-- bitemporal live-table exclusion constraints. All are stock
-- Postgres contrib modules.
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;
CREATE EXTENSION IF NOT EXISTS btree_gist;

-- Roles that `okg migrate` will also create, created here WITHOUT login.
-- Earlier releases gave them LOGIN with fixed passwords committed in source,
-- so any account that could reach the server could log in as them (issue
-- #3273). `okg db role-passwords --apply` gives okg_mcp and app_rw LOGIN with
-- random passwords and records them in an owner-only file `okg mcp-serve`
-- reads; `okg migrate` (views.sql) applies okg_mcp's grants and app_ro
-- membership.
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_rw') THEN
    CREATE ROLE app_rw NOLOGIN;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_ro') THEN
    CREATE ROLE app_ro NOLOGIN;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'catalog_admin') THEN
    CREATE ROLE catalog_admin NOLOGIN;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'okg_mcp') THEN
    CREATE ROLE okg_mcp NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE;
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
