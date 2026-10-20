-- Role enforcement test: app_ro is strictly read-only on okg.* AND
-- only sees the agent-facing view layer (`okg.v_*`).
--
-- Per `harden-mcp-run-sql-safety` (2026-04-27): the prior version
-- of this test asserted `app_ro` could SELECT from physical tables
-- (`nodes_live`, `edges_live`, `graph_generations`, `subtypes`,
-- `narrowings`, `aliases`). That contradicted the spec. The new
-- contract: `app_ro` can read only `okg.v_*` views and the
-- `okg.current_pin()` helper function. Every other relation in
-- schema okg is inaccessible.
--
-- Fail-loud discipline: each expected-fail path RAISE EXCEPTIONs
-- if the forbidden operation is silently allowed. ON_ERROR_STOP is
-- on, so an unexpected permission denied on a SELECT aborts the
-- script.

\set ON_ERROR_STOP on

-- ========== assert_ok: app_ro has SELECT on every REGISTERED relation ======
--
-- Graph views execute only inside an application-created Chronos checkout.
-- A bare SQL session cannot create that checkout, so executing every view here
-- would test an unsupported read path and fail even when the role grants are
-- correct. Verify the grant matrix directly; checkout-backed execution is
-- covered by the MCP/operator integration tests.
--
-- The set walked here used to be "every relation NAMED okg.v_*". That was the
-- naming convention `schema-grant-coherence` removed: it made any
-- migration-, deployment- or test-created `v_something` part of the published
-- read surface, and made this assertion fail whenever such a view existed
-- without a grant. The set is now okg.grant_registry's app_ro rows, which is
-- the decision record. A `v_`-named view with NO row is a separate finding,
-- reported by okg.grant_registry_drift().

-- NOT run under `SET LOCAL ROLE app_ro`, deliberately: app_ro cannot read
-- okg.grant_registry, and it should not be able to — the registry is the
-- decision record, not part of the published read surface. Naming the role in
-- has_table_privilege asks the same question without needing to become it.

BEGIN;

DO $$
DECLARE
  rec record;
BEGIN
  IF to_regclass('okg.grant_registry') IS NULL THEN
    RAISE NOTICE 'SKIP: okg.grant_registry absent (pre-0027 database)';
    RETURN;
  END IF;
  FOR rec IN
    SELECT g.relation,
           has_table_privilege('app_ro', to_regclass(g.relation), 'SELECT')
             AS can_read
      FROM okg.grant_registry g
     WHERE g.role_name = 'app_ro'
       AND g.classification = 'contract'
       AND g.object_kind = 'TABLE'
       AND NOT g.is_pattern
       AND to_regclass(g.relation) IS NOT NULL
     ORDER BY g.relation
  LOOP
    IF NOT rec.can_read THEN
      RAISE EXCEPTION 'FAIL: app_ro lacks SELECT on %', rec.relation;
    END IF;
    RAISE NOTICE 'OK: app_ro has SELECT on %', rec.relation;
  END LOOP;
END
$$;

ROLLBACK;

-- ========== assert_denied: app_ro cannot SELECT physical tables ==========
--
-- Current physical graph/runtime tables MUST raise insufficient_privilege under
-- SELECT. The retired `nodes_live` / `edges_live` tables are covered by
-- tests/sql/schema/schema_contracts.sql instead of referenced here.

BEGIN;
SET LOCAL ROLE app_ro;
DO $$
BEGIN
  PERFORM count(*) FROM okg.graph_nodes;
  RAISE EXCEPTION 'FAIL: app_ro was unexpectedly allowed to SELECT from okg.graph_nodes';
EXCEPTION
  WHEN insufficient_privilege THEN
    RAISE NOTICE 'OK: app_ro denied SELECT on okg.graph_nodes';
END
$$;
ROLLBACK;

BEGIN;
SET LOCAL ROLE app_ro;
DO $$
BEGIN
  PERFORM count(*) FROM okg.graph_edges;
  RAISE EXCEPTION 'FAIL: app_ro was unexpectedly allowed to SELECT from okg.graph_edges';
EXCEPTION
  WHEN insufficient_privilege THEN
    RAISE NOTICE 'OK: app_ro denied SELECT on okg.graph_edges';
END
$$;
ROLLBACK;

BEGIN;
SET LOCAL ROLE app_ro;
DO $$
BEGIN
  PERFORM count(*) FROM okg.graph_generations;
  RAISE EXCEPTION 'FAIL: app_ro was unexpectedly allowed to SELECT from okg.graph_generations';
EXCEPTION
  WHEN insufficient_privilege THEN
    RAISE NOTICE 'OK: app_ro denied SELECT on okg.graph_generations';
END
$$;
ROLLBACK;

BEGIN;
SET LOCAL ROLE app_ro;
DO $$
BEGIN
  PERFORM count(*) FROM okg.subtypes;
  RAISE EXCEPTION 'FAIL: app_ro was unexpectedly allowed to SELECT from okg.subtypes';
EXCEPTION
  WHEN insufficient_privilege THEN
    RAISE NOTICE 'OK: app_ro denied SELECT on okg.subtypes';
END
$$;
ROLLBACK;

-- The previously-leaky tables (P0-1 in the design review):
-- invariants, catalog_versions, link_cache, source_watermarks,
-- mcp_trace_events, trace_sessions, trace_backtracks. After the
-- default-deny rewrite, all of these MUST also be denied.

BEGIN;
SET LOCAL ROLE app_ro;
DO $$
BEGIN
  PERFORM count(*) FROM okg.invariants;
  RAISE EXCEPTION 'FAIL: app_ro was unexpectedly allowed to SELECT from okg.invariants';
EXCEPTION
  WHEN insufficient_privilege THEN
    RAISE NOTICE 'OK: app_ro denied SELECT on okg.invariants';
END
$$;
ROLLBACK;

BEGIN;
SET LOCAL ROLE app_ro;
DO $$
BEGIN
  PERFORM count(*) FROM okg.catalog_versions;
  RAISE EXCEPTION 'FAIL: app_ro was unexpectedly allowed to SELECT from okg.catalog_versions';
EXCEPTION
  WHEN insufficient_privilege THEN
    RAISE NOTICE 'OK: app_ro denied SELECT on okg.catalog_versions';
END
$$;
ROLLBACK;

BEGIN;
SET LOCAL ROLE app_ro;
DO $$
BEGIN
  PERFORM count(*) FROM okg.mcp_trace_events;
  RAISE EXCEPTION 'FAIL: app_ro was unexpectedly allowed to SELECT from okg.mcp_trace_events';
EXCEPTION
  WHEN insufficient_privilege THEN
    RAISE NOTICE 'OK: app_ro denied SELECT on okg.mcp_trace_events';
END
$$;
ROLLBACK;

-- ========== assert_denied: app_ro cannot INSERT anywhere ==========

BEGIN;
SET LOCAL ROLE app_ro;
DO $$
BEGIN
  INSERT INTO okg.node_facts
    (fact_id, node_id, subtype, attrs, source, source_record_id,
     source_revision, observed_at, dedupe_key, run_id, op)
  VALUES
    (-91001, 'test:ro_illegal', 'Probe', '{}'::jsonb, '_ro_probe',
     '{}'::jsonb, '{}'::jsonb, now(),
     md5(random()::text), gen_random_uuid(), 'I');
  RAISE EXCEPTION 'FAIL: app_ro was unexpectedly allowed to write okg.node_facts';
EXCEPTION
  WHEN insufficient_privilege THEN
    RAISE NOTICE 'OK: app_ro denied INSERT on okg.node_facts';
END
$$;
ROLLBACK;

BEGIN;
SET LOCAL ROLE app_ro;
DO $$
BEGIN
  INSERT INTO okg.subtypes (name, metadata, catalog_version_id)
  VALUES ('_ro_illegal', '{}'::jsonb, 1);
  RAISE EXCEPTION 'FAIL: app_ro was unexpectedly allowed to write okg.subtypes';
EXCEPTION
  WHEN insufficient_privilege THEN
    RAISE NOTICE 'OK: app_ro denied INSERT on okg.subtypes';
END
$$;
ROLLBACK;

BEGIN;
SET LOCAL ROLE app_ro;
DO $$
BEGIN
  INSERT INTO okg.catalog_versions
    (catalog_version_id, status, applied_at, diff_summary, reconcile_plan)
  VALUES (-91001, 'pending', now(), '{}'::jsonb, '{}'::jsonb);
  RAISE EXCEPTION 'FAIL: app_ro was unexpectedly allowed to write okg.catalog_versions';
EXCEPTION
  WHEN insufficient_privilege THEN
    RAISE NOTICE 'OK: app_ro denied INSERT on okg.catalog_versions';
END
$$;
ROLLBACK;

-- ========== assert_denied: app_ro cannot UPDATE ==========

BEGIN;
SET LOCAL ROLE app_ro;
DO $$
DECLARE n int;
BEGIN
  UPDATE okg.graph_generations SET status = 'blocked'
   WHERE status = 'published';
  GET DIAGNOSTICS n = ROW_COUNT;
  RAISE EXCEPTION 'FAIL: app_ro was unexpectedly allowed to UPDATE '
                  'okg.graph_generations (affected % rows)', n;
EXCEPTION
  WHEN insufficient_privilege THEN
    RAISE NOTICE 'OK: app_ro denied UPDATE on okg.graph_generations';
END
$$;
ROLLBACK;

-- ========== assert_denied: app_ro cannot DELETE ==========

BEGIN;
SET LOCAL ROLE app_ro;
DO $$
BEGIN
  DELETE FROM okg.node_facts WHERE node_id LIKE 'test:%';
  RAISE EXCEPTION 'FAIL: app_ro was unexpectedly allowed to DELETE from okg.node_facts';
EXCEPTION
  WHEN insufficient_privilege THEN
    RAISE NOTICE 'OK: app_ro denied DELETE on okg.node_facts';
END
$$;
ROLLBACK;

-- ========== assert_denied: app_ro cannot DDL ==========

BEGIN;
SET LOCAL ROLE app_ro;
DO $$
BEGIN
  CREATE TABLE okg._ro_illegal (id int);
  RAISE EXCEPTION 'FAIL: app_ro was unexpectedly allowed to CREATE TABLE in okg';
EXCEPTION
  WHEN insufficient_privilege THEN
    RAISE NOTICE 'OK: app_ro denied CREATE TABLE in okg';
END
$$;
ROLLBACK;

\echo ''
\echo 'app_ro_read_only.sql: role reads okg.v_* only and is strictly read-only as required.'
