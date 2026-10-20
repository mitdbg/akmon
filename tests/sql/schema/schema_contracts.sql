-- Schema contract assertions that are easier to state in SQL than Python.
-- Run against a migrated database as a superuser/test owner.

\set ON_ERROR_STOP on

DO $$
DECLARE missing text[];
BEGIN
  SELECT array_agg(name ORDER BY name) INTO missing
  FROM (
    VALUES
      ('graph_nodes'),
      ('graph_edges'),
      ('graph_generations'),
      ('node_facts'),
      ('edge_facts'),
      ('source_watermarks')
  ) AS expected(name)
  WHERE to_regclass(format('okg.%I', name)) IS NULL;

  IF missing IS NOT NULL THEN
    RAISE EXCEPTION 'missing required okg schema relations: %', missing;
  END IF;
END
$$;

DO $$
DECLARE present text[];
BEGIN
  SELECT array_agg(name ORDER BY name) INTO present
  FROM (
    VALUES
      ('nodes_live'),
      ('edges_live'),
      ('jobs'),
      ('publisher_leases'),
      -- schema-canonical-read-path. The history pair and v_changes are
      -- decision D6 (real history views are post-OSS additive work); the
      -- projectable pair went with the live-table projection that was their
      -- only consumer. Each returned rows an agent could not trust: live rows
      -- under constant-NULL generation bounds, or no rows at all from a
      -- `WHERE false` stub.
      ('v_nodes_history'),
      ('v_edges_history'),
      ('v_changes'),
      ('projectable_node_facts'),
      ('projectable_edge_facts')
  ) AS legacy(name)
  WHERE to_regclass(format('okg.%I', name)) IS NOT NULL;

  IF present IS NOT NULL THEN
    RAISE EXCEPTION
      'retired legacy runtime/live/read relations must stay retired: %',
      present;
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint c
    JOIN pg_class r ON r.oid = c.conrelid
    JOIN pg_namespace n ON n.oid = r.relnamespace
    WHERE n.nspname = 'okg'
      AND r.relname = 'graph_generations'
      AND c.contype = 'c'
      AND c.conname = 'graph_generations_status_chk'
  ) THEN
    RAISE EXCEPTION
      'okg.graph_generations must expose graph_generations_status_chk';
  END IF;
END
$$;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.role_table_grants
    WHERE table_schema = 'okg'
      AND grantee = 'app_ro'
      AND privilege_type <> 'SELECT'
  ) THEN
    RAISE EXCEPTION 'app_ro has non-SELECT table privileges in okg schema';
  END IF;
END
$$;

-- constrain-mcp-and-role-bootstrap (#366 / F-04): okg_mcp may read the direct
-- graph/catalog/status surfaces it is explicitly granted, but the sensitive raw
-- fact logs, Nomos policy config, federation mailbox, source cursor blobs, and
-- Poros queue tables are NOT part of the default MCP read boundary. This is the
-- regression guard for the blanket `GRANT SELECT ON ALL TABLES ... TO okg_mcp`
-- that this PR replaced with an explicit allow-list.
DO $$
DECLARE leaks text;
BEGIN
  SELECT string_agg(relation, ', ' ORDER BY relation)
    INTO leaks
    FROM (VALUES
      ('okg.node_facts'),
      ('okg.edge_facts'),
      ('okg.nomos_config_sets'),
      ('okg.poros_outbox_messages'),
      ('okg.federation_mailbox_messages'),
      ('okg.source_cursor_blobs')
    ) AS sensitive(relation)
   WHERE to_regclass(relation) IS NOT NULL
     AND has_table_privilege('okg_mcp', relation, 'SELECT');

  IF leaks IS NOT NULL THEN
    RAISE EXCEPTION 'FAIL: okg_mcp can SELECT sensitive raw tables: %', leaks;
  END IF;
  RAISE NOTICE 'OK: okg_mcp sensitive raw-table SELECT grants are denied';
END
$$;

-- Future non-Chronos public tables must not become readable just because they
-- are created. Chronos `_chronos*` tables get explicit catch-up grants from the
-- graph-store ensure/write path instead of broad public default privileges, so
-- a freshly-created arbitrary public table must stay unreadable to okg_mcp.
DROP TABLE IF EXISTS public.mcp_default_leak;
CREATE TABLE public.mcp_default_leak (id int);
DO $$
BEGIN
  IF has_table_privilege('okg_mcp', 'public.mcp_default_leak', 'SELECT') THEN
    RAISE EXCEPTION 'FAIL: okg_mcp can SELECT future non-Chronos public table';
  END IF;
  RAISE NOTICE 'OK: okg_mcp cannot SELECT future non-Chronos public tables by default';
END
$$;
DROP TABLE public.mcp_default_leak;

\echo ''
\echo 'schema_contracts.sql: current graph/runtime shape, app_ro grants, and okg_mcp read boundary verified.'
