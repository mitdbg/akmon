-- Role enforcement test: app_rw must be able to write the runtime
-- tables and MUST NOT be able to write the catalog tables or do DDL.
--
-- Run as a superuser (or `postgres`); the test uses SET LOCAL ROLE
-- to switch to app_rw for each check.
--
-- Fail-loud discipline: every `assert_ok` block runs inside a
-- DO/EXCEPTION block that re-raises any unexpected error. An
-- unexpected permission denied aborts the test with a clear
-- message rather than silently aborting the surrounding
-- transaction and printing a bogus success banner.

\set ON_ERROR_STOP on

-- ========== assert_ok: app_rw can write non-fact runtime tables ==========
--
-- Direct writes to node_facts / edge_facts are now commit-witness protected.
-- Raw graph_generations inserts also cross the branch-resolution trigger, so
-- publisher-specific tests cover that path. This role assertion focuses on
-- operational runtime rows that app_rw can write directly.
BEGIN;
SET LOCAL ROLE app_rw;

DO $$
BEGIN
  INSERT INTO okg.source_watermarks
    (source, cutover_watermark, last_applied_rev,
     last_published_rev, projection_cursor, last_reconcile_at)
  VALUES
    ('_rw_probe', '{}'::jsonb, '{}'::jsonb, '{}'::jsonb, 0, now())
  ON CONFLICT (source) DO UPDATE SET projection_cursor = EXCLUDED.projection_cursor;
  RAISE NOTICE 'OK: app_rw INSERT/UPDATE on okg.source_watermarks';
END
$$;

DO $$
BEGIN
  UPDATE okg.source_watermarks
     SET last_applied_rev = '{"v":99}'::jsonb
   WHERE source = '_rw_probe';
  RAISE NOTICE 'OK: app_rw UPDATE on okg.source_watermarks';
END
$$;

DO $$
BEGIN
  INSERT INTO okg.link_cache (node_id, text_hash, linked_at)
  VALUES ('test:role_probe_rw', md5('x'), now())
  ON CONFLICT (node_id) DO UPDATE SET text_hash = EXCLUDED.text_hash;
  RAISE NOTICE 'OK: app_rw INSERT/UPDATE on okg.link_cache';
END
$$;

DO $$
BEGIN
  INSERT INTO okg.mcp_trace_events
    (session_id, deployment_name, tool_name, request_json)
  VALUES ('role_probe_rw', 'test', 'role-probe', '{}'::jsonb);
  RAISE NOTICE 'OK: app_rw INSERT on okg.mcp_trace_events';
END
$$;

DO $$
BEGIN
  -- A zero-row delete still exercises PostgreSQL's table privilege check.
  DELETE FROM okg.telemetry_graph_effects
   WHERE deployment_name = '_rw_probe';
  RAISE NOTICE 'OK: app_rw DELETE on bounded graph-effect identity retention';
END
$$;

ROLLBACK;

-- ========== assert_denied: app_rw cannot write the catalog ==========
-- Each denial runs in its OWN transaction. If a denial fails to fire
-- (i.e., the INSERT silently succeeds), the test aborts with a clear
-- error rather than proceeding.

BEGIN;
SET LOCAL ROLE app_rw;
DO $$
BEGIN
  INSERT INTO okg.subtypes (name, metadata, catalog_version_id)
  VALUES ('_rw_illegal', '{}'::jsonb, 1);
  RAISE EXCEPTION 'FAIL: app_rw was unexpectedly allowed to write okg.subtypes';
EXCEPTION
  WHEN insufficient_privilege THEN
    RAISE NOTICE 'OK: app_rw denied INSERT on okg.subtypes';
END
$$;
ROLLBACK;

BEGIN;
SET LOCAL ROLE app_rw;
DO $$
BEGIN
  INSERT INTO okg.narrowings
    (parent_subtype, edge_type, child_subtype, attr_rules, catalog_version_id)
  VALUES ('A', 'e', 'B', '{}'::jsonb, 1);
  RAISE EXCEPTION 'FAIL: app_rw was unexpectedly allowed to write okg.narrowings';
EXCEPTION
  WHEN insufficient_privilege THEN
    RAISE NOTICE 'OK: app_rw denied INSERT on okg.narrowings';
END
$$;
ROLLBACK;

BEGIN;
SET LOCAL ROLE app_rw;
DO $$
BEGIN
  INSERT INTO okg.aliases (alias, canonical, scope, catalog_version_id)
  VALUES ('_rw_illegal_alias', 'node:x', NULL, 1);
  RAISE EXCEPTION 'FAIL: app_rw was unexpectedly allowed to write okg.aliases';
EXCEPTION
  WHEN insufficient_privilege THEN
    RAISE NOTICE 'OK: app_rw denied INSERT on okg.aliases';
END
$$;
ROLLBACK;

BEGIN;
SET LOCAL ROLE app_rw;
DO $$
BEGIN
  INSERT INTO okg.invariants
    (name, severity, scope, sql_body, catalog_version_id)
  VALUES ('_rw_illegal', 'error', 'cross', 'SELECT 1 WHERE false', 1);
  RAISE EXCEPTION 'FAIL: app_rw was unexpectedly allowed to write okg.invariants';
EXCEPTION
  WHEN insufficient_privilege THEN
    RAISE NOTICE 'OK: app_rw denied INSERT on okg.invariants';
END
$$;
ROLLBACK;

BEGIN;
SET LOCAL ROLE app_rw;
DO $$
BEGIN
  INSERT INTO okg.catalog_versions
    (status, applied_at, diff_summary, reconcile_plan)
  VALUES ('pending', now(), '{}'::jsonb, '{}'::jsonb);
  RAISE EXCEPTION 'FAIL: app_rw was unexpectedly allowed to write okg.catalog_versions';
EXCEPTION
  WHEN insufficient_privilege THEN
    RAISE NOTICE 'OK: app_rw denied INSERT on okg.catalog_versions';
END
$$;
ROLLBACK;

-- ========== assert_denied: app_rw cannot DDL ==========

BEGIN;
SET LOCAL ROLE app_rw;
DO $$
BEGIN
  CREATE TABLE okg._rw_illegal (id int);
  RAISE EXCEPTION 'FAIL: app_rw was unexpectedly allowed to CREATE TABLE in okg';
EXCEPTION
  WHEN insufficient_privilege THEN
    RAISE NOTICE 'OK: app_rw denied CREATE TABLE in okg';
END
$$;
ROLLBACK;

BEGIN;
SET LOCAL ROLE app_rw;
DO $$
BEGIN
  ALTER TABLE okg.node_facts ADD COLUMN _rw_illegal int;
  RAISE EXCEPTION 'FAIL: app_rw was unexpectedly allowed to ALTER TABLE okg.node_facts';
EXCEPTION
  WHEN insufficient_privilege THEN
    RAISE NOTICE 'OK: app_rw denied ALTER TABLE on okg.node_facts';
END
$$;
ROLLBACK;

BEGIN;
SET LOCAL ROLE app_rw;
DO $$
BEGIN
  TRUNCATE okg.node_facts;
  RAISE EXCEPTION 'FAIL: app_rw was unexpectedly allowed to TRUNCATE okg.node_facts';
EXCEPTION
  WHEN insufficient_privilege THEN
    RAISE NOTICE 'OK: app_rw denied TRUNCATE on okg.node_facts';
END
$$;
ROLLBACK;

-- ========== assert_denied: app_rw cannot DELETE facts ==========

BEGIN;
SET LOCAL ROLE app_rw;
DO $$
BEGIN
  DELETE FROM okg.node_facts WHERE node_id LIKE 'test:%';
  RAISE EXCEPTION 'FAIL: app_rw was unexpectedly allowed to DELETE from okg.node_facts';
EXCEPTION
  WHEN insufficient_privilege THEN
    RAISE NOTICE 'OK: app_rw denied DELETE on okg.node_facts';
END
$$;
ROLLBACK;

-- ========== vector sets (PACT openrouter-embeddings-hnsw) ==========
-- app_rw inserts vectors, links and watermarks and moves a set's counter;
-- it can neither rewrite nor delete them, nor register or rename a set.

BEGIN;
INSERT INTO okg.embedding_sets
  (set_id, name, backend, identity_json, width, documents_json, index_json)
VALUES ('es_000000000000000000000000', '_rw_probe', 'hash', '{}'::jsonb, 3,
        '{}'::jsonb, '{}'::jsonb);
INSERT INTO okg.embedding_vector_counters (set_id)
VALUES ('es_000000000000000000000000');
SET LOCAL ROLE app_rw;
DO $$
BEGIN
  UPDATE okg.embedding_sets SET next_seq = next_seq + 3
   WHERE set_id = 'es_000000000000000000000000';
  UPDATE okg.embedding_vector_counters SET next_seq = next_seq + 1
   WHERE set_id = 'es_000000000000000000000000';
  INSERT INTO okg.text_embeddings (set_id, doc_hash, embedding, seq)
  VALUES ('es_000000000000000000000000', repeat('a', 64), '[1,0,0]'::halfvec, 1);
  INSERT INTO okg.node_documents
    (set_id, template_hash, node_id, row_hash, doc_hash, seq)
  VALUES ('es_000000000000000000000000', repeat('b', 64), 'n:1',
          repeat('c', 64), repeat('a', 64), 2);
  INSERT INTO okg.embedding_watermarks
    (generation_id, set_id, max_seq, recorded_by)
  VALUES ('_rw_probe', 'es_000000000000000000000000', 3, 'publisher');
  INSERT INTO okg.embedding_templates (set_id, template_hash, subtype, record_json)
  VALUES ('es_000000000000000000000000', repeat('b', 64), 'note', '{}'::jsonb)
  ON CONFLICT (set_id, template_hash) DO NOTHING;
  PERFORM 1 FROM okg.text_embeddings, okg.node_documents, okg.embedding_watermarks,
                 okg.embedding_templates;
  RAISE NOTICE 'OK: app_rw INSERT on vectors, links, watermarks, templates and both set counters';
END
$$;
ROLLBACK;

BEGIN;
INSERT INTO okg.embedding_sets
  (set_id, name, backend, identity_json, width, documents_json, index_json)
VALUES ('es_000000000000000000000000', '_rw_probe', 'hash', '{}'::jsonb, 3,
        '{}'::jsonb, '{}'::jsonb);
INSERT INTO okg.text_embeddings (set_id, doc_hash, embedding, seq)
VALUES ('es_000000000000000000000000', repeat('a', 64), '[1,0,0]'::halfvec, 1);
SET LOCAL ROLE app_rw;
DO $$
BEGIN
  UPDATE okg.text_embeddings SET seq = 2 WHERE set_id = 'es_000000000000000000000000';
  RAISE EXCEPTION 'FAIL: app_rw was unexpectedly allowed to UPDATE okg.text_embeddings';
EXCEPTION
  WHEN insufficient_privilege THEN
    RAISE NOTICE 'OK: app_rw denied UPDATE on okg.text_embeddings';
END
$$;
ROLLBACK;

BEGIN;
SET LOCAL ROLE app_rw;
DO $$
BEGIN
  DELETE FROM okg.text_embeddings;
  RAISE EXCEPTION 'FAIL: app_rw was unexpectedly allowed to DELETE from okg.text_embeddings';
EXCEPTION
  WHEN insufficient_privilege THEN
    RAISE NOTICE 'OK: app_rw denied DELETE on okg.text_embeddings';
END
$$;
ROLLBACK;

BEGIN;
SET LOCAL ROLE app_rw;
DO $$
BEGIN
  DELETE FROM okg.node_documents;
  RAISE EXCEPTION 'FAIL: app_rw was unexpectedly allowed to DELETE from okg.node_documents';
EXCEPTION
  WHEN insufficient_privilege THEN
    RAISE NOTICE 'OK: app_rw denied DELETE on okg.node_documents';
END
$$;
ROLLBACK;

BEGIN;
SET LOCAL ROLE app_rw;
DO $$
BEGIN
  UPDATE okg.node_documents SET seq = 1;
  RAISE EXCEPTION 'FAIL: app_rw was unexpectedly allowed to UPDATE okg.node_documents';
EXCEPTION
  WHEN insufficient_privilege THEN
    RAISE NOTICE 'OK: app_rw denied UPDATE on okg.node_documents';
END
$$;
ROLLBACK;

BEGIN;
SET LOCAL ROLE app_rw;
DO $$
BEGIN
  DELETE FROM okg.embedding_templates;
  RAISE EXCEPTION 'FAIL: app_rw was unexpectedly allowed to DELETE from okg.embedding_templates';
EXCEPTION
  WHEN insufficient_privilege THEN
    RAISE NOTICE 'OK: app_rw denied DELETE on okg.embedding_templates';
END
$$;
ROLLBACK;

BEGIN;
SET LOCAL ROLE app_rw;
DO $$
BEGIN
  UPDATE okg.embedding_templates SET subtype = 'x';
  RAISE EXCEPTION 'FAIL: app_rw was unexpectedly allowed to UPDATE okg.embedding_templates';
EXCEPTION
  WHEN insufficient_privilege THEN
    RAISE NOTICE 'OK: app_rw denied UPDATE on okg.embedding_templates';
END
$$;
ROLLBACK;

BEGIN;
SET LOCAL ROLE app_rw;
DO $$
BEGIN
  UPDATE okg.embedding_watermarks SET max_seq = 0;
  RAISE EXCEPTION 'FAIL: app_rw was unexpectedly allowed to UPDATE okg.embedding_watermarks';
EXCEPTION
  WHEN insufficient_privilege THEN
    RAISE NOTICE 'OK: app_rw denied UPDATE on okg.embedding_watermarks';
END
$$;
ROLLBACK;

BEGIN;
SET LOCAL ROLE app_rw;
DO $$
BEGIN
  INSERT INTO okg.embedding_sets
    (set_id, name, backend, identity_json, width, documents_json, index_json)
  VALUES ('es_000000000000000000000001', '_rw_illegal', 'hash', '{}'::jsonb, 3,
          '{}'::jsonb, '{}'::jsonb);
  RAISE EXCEPTION 'FAIL: app_rw was unexpectedly allowed to register a vector set';
EXCEPTION
  WHEN insufficient_privilege THEN
    RAISE NOTICE 'OK: app_rw denied INSERT on okg.embedding_sets';
END
$$;
ROLLBACK;

BEGIN;
SET LOCAL ROLE app_rw;
DO $$
BEGIN
  UPDATE okg.embedding_sets SET active = FALSE;
  RAISE EXCEPTION 'FAIL: app_rw was unexpectedly allowed to change a set''s registration';
EXCEPTION
  WHEN insufficient_privilege THEN
    RAISE NOTICE 'OK: app_rw denied UPDATE of okg.embedding_sets registration columns';
END
$$;
ROLLBACK;

BEGIN;
INSERT INTO okg.embedding_sets
  (set_id, name, backend, identity_json, width, documents_json, index_json)
VALUES ('es_000000000000000000000000', '_rw_probe', 'hash', '{}'::jsonb, 3,
        '{}'::jsonb, '{}'::jsonb);
SET LOCAL ROLE app_rw;
DO $$
BEGIN
  INSERT INTO okg.embedding_vector_counters (set_id)
  VALUES ('es_000000000000000000000000');
  RAISE EXCEPTION 'FAIL: app_rw was unexpectedly allowed to add a vector counter';
EXCEPTION
  WHEN insufficient_privilege THEN
    RAISE NOTICE 'OK: app_rw denied INSERT on okg.embedding_vector_counters';
END
$$;
ROLLBACK;

BEGIN;
SET LOCAL ROLE app_rw;
DO $$
BEGIN
  DELETE FROM okg.embedding_vector_counters;
  RAISE EXCEPTION 'FAIL: app_rw was unexpectedly allowed to DELETE a vector counter';
EXCEPTION
  WHEN insufficient_privilege THEN
    RAISE NOTICE 'OK: app_rw denied DELETE on okg.embedding_vector_counters';
END
$$;
ROLLBACK;

BEGIN;
INSERT INTO okg.embedding_sets
  (set_id, name, backend, identity_json, width, documents_json, index_json)
VALUES ('es_000000000000000000000000', '_rw_probe', 'hash', '{}'::jsonb, 3,
        '{}'::jsonb, '{}'::jsonb);
SET LOCAL ROLE app_rw;
DO $$
BEGIN
  INSERT INTO okg.embedding_fill_completions
    (set_id, generation_id, max_seq, max_vector_seq)
  VALUES ('es_000000000000000000000000', '_rw_probe', 0, 0);
  PERFORM 1 FROM okg.embedding_fill_completions;
  RAISE NOTICE 'OK: app_rw INSERT on okg.embedding_fill_completions';
END
$$;
DO $$
BEGIN
  DELETE FROM okg.embedding_fill_completions;
  RAISE EXCEPTION 'FAIL: app_rw was unexpectedly allowed to DELETE a fill completion';
EXCEPTION
  WHEN insufficient_privilege THEN
    RAISE NOTICE 'OK: app_rw denied DELETE on okg.embedding_fill_completions';
END
$$;
ROLLBACK;

\echo ''
\echo 'app_rw_grants.sql: all grants and denials held.'
