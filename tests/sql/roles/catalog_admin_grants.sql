-- Role enforcement test: catalog_admin writes the ontology catalog
-- tables (including catalog_versions) and MUST NOT write the
-- runtime/fact tables or do DDL.

\set ON_ERROR_STOP on

-- ========== assert_ok: catalog_admin writes the catalog ==========
BEGIN;
SET LOCAL ROLE catalog_admin;

-- catalog_admin MUST be able to write catalog_versions (the
-- reconcile-plan migrator runs as catalog_admin).
DO $$
DECLARE v int;
BEGIN
  INSERT INTO okg.catalog_versions
    (status, applied_at, diff_summary, reconcile_plan)
  VALUES
    ('pending', now(), '{}'::jsonb,
     '{"applied_count":0,"reconcile":[]}'::jsonb)
  RETURNING catalog_version_id INTO v;
  RAISE NOTICE 'OK: catalog_admin INSERT on okg.catalog_versions (id=%)', v;
  -- Scratch across the following inserts via a SAVEPOINT so we can
  -- reference this catalog_version_id.
  PERFORM set_config('test.catalog_v', v::text, true);
END
$$;

DO $$
DECLARE v int := current_setting('test.catalog_v')::int;
BEGIN
  INSERT INTO okg.subtypes (name, metadata, catalog_version_id)
  VALUES ('_admin_probe', '{}'::jsonb, v);
  RAISE NOTICE 'OK: catalog_admin INSERT on okg.subtypes';
END
$$;

DO $$
DECLARE v int := current_setting('test.catalog_v')::int;
BEGIN
  INSERT INTO okg.narrowings
    (parent_subtype, edge_type, child_subtype, attr_rules, catalog_version_id)
  VALUES ('_admin_probe', 'test_edge', '_admin_probe', '{}'::jsonb, v);
  RAISE NOTICE 'OK: catalog_admin INSERT on okg.narrowings';
END
$$;

DO $$
DECLARE v int := current_setting('test.catalog_v')::int;
BEGIN
  INSERT INTO okg.aliases (alias, canonical, scope, catalog_version_id)
  VALUES ('_admin_probe_alias', 'node:x', NULL, v);
  RAISE NOTICE 'OK: catalog_admin INSERT on okg.aliases';
END
$$;

DO $$
DECLARE v int := current_setting('test.catalog_v')::int;
BEGIN
  INSERT INTO okg.invariants
    (name, severity, scope, sql_body, catalog_version_id)
  VALUES ('_admin_probe', 'warn', 'cross',
          'SELECT 1 WHERE false LIMIT 100', v);
  RAISE NOTICE 'OK: catalog_admin INSERT on okg.invariants';
END
$$;

-- catalog_admin MUST be able to UPDATE catalog_versions to transition
-- status from pending to applied.
DO $$
DECLARE v int := current_setting('test.catalog_v')::int;
BEGIN
  UPDATE okg.catalog_versions SET status = 'applied'
   WHERE catalog_version_id = v;
  RAISE NOTICE 'OK: catalog_admin UPDATE on okg.catalog_versions';
END
$$;

ROLLBACK;

-- ========== assert_denied: catalog_admin cannot write runtime ==========

BEGIN;
SET LOCAL ROLE catalog_admin;
DO $$
BEGIN
  INSERT INTO okg.node_facts
    (fact_id, node_id, subtype, attrs, source, source_record_id,
     source_revision, observed_at, dedupe_key, run_id, op)
  VALUES
    (-92001, 'test:admin_illegal', 'Probe', '{}'::jsonb, '_admin_probe',
     '{}'::jsonb, '{}'::jsonb, now(),
     md5(random()::text), gen_random_uuid(), 'I');
  RAISE EXCEPTION 'FAIL: catalog_admin was unexpectedly allowed to write okg.node_facts';
EXCEPTION
  WHEN insufficient_privilege THEN
    RAISE NOTICE 'OK: catalog_admin denied INSERT on okg.node_facts';
END
$$;
ROLLBACK;

BEGIN;
SET LOCAL ROLE catalog_admin;
DO $$
BEGIN
  INSERT INTO okg.edge_facts
    (fact_id, edge_id, src, dst, edge_type, attrs, provenance, confidence,
     source, source_record_id, source_revision, observed_at,
     dedupe_key, run_id, op)
  VALUES
    (-92002, -92002, 'test:a', 'test:b',
     'test_edge', '{}'::jsonb, 'explicit', 1.0, '_admin_probe',
     '{}'::jsonb, '{}'::jsonb, now(),
     md5(random()::text), gen_random_uuid(), 'I');
  RAISE EXCEPTION 'FAIL: catalog_admin was unexpectedly allowed to write okg.edge_facts';
EXCEPTION
  WHEN insufficient_privilege THEN
    RAISE NOTICE 'OK: catalog_admin denied INSERT on okg.edge_facts';
END
$$;
ROLLBACK;

BEGIN;
SET LOCAL ROLE catalog_admin;
DO $$
BEGIN
  INSERT INTO okg.source_watermarks
    (source, cutover_watermark, last_applied_rev,
     last_published_rev, projection_cursor, last_reconcile_at)
  VALUES ('_admin_illegal', '{}'::jsonb, '{}'::jsonb, '{}'::jsonb, 0, now());
  RAISE EXCEPTION 'FAIL: catalog_admin was unexpectedly allowed to write okg.source_watermarks';
EXCEPTION
  WHEN insufficient_privilege THEN
    RAISE NOTICE 'OK: catalog_admin denied INSERT on okg.source_watermarks';
END
$$;
ROLLBACK;

BEGIN;
SET LOCAL ROLE catalog_admin;
DO $$
BEGIN
  INSERT INTO okg.graph_generations
    (generation_id, deployment_name, status, catalog_version_id,
     fact_watermark, published_at, invariant_summary, excluded_sources,
     correlation_id)
  VALUES
    (-92003, 'role-test', 'pending', 1, 0, now(), '{}'::jsonb, '[]'::jsonb,
     'catalog-admin-denied');
  RAISE EXCEPTION 'FAIL: catalog_admin was unexpectedly allowed to write okg.graph_generations';
EXCEPTION
  WHEN insufficient_privilege THEN
    RAISE NOTICE 'OK: catalog_admin denied INSERT on okg.graph_generations';
END
$$;
ROLLBACK;

-- ========== assert_denied: catalog_admin cannot DDL ==========

BEGIN;
SET LOCAL ROLE catalog_admin;
DO $$
BEGIN
  CREATE TABLE okg._admin_illegal (id int);
  RAISE EXCEPTION 'FAIL: catalog_admin was unexpectedly allowed to CREATE TABLE in okg';
EXCEPTION
  WHEN insufficient_privilege THEN
    RAISE NOTICE 'OK: catalog_admin denied CREATE TABLE in okg';
END
$$;
ROLLBACK;

\echo ''
\echo 'catalog_admin_grants.sql: catalog writes permitted; runtime writes and DDL denied.'
