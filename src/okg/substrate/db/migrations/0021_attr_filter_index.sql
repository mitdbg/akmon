SET LOCAL lock_timeout = '10s';
SET LOCAL statement_timeout = '30min';

-- PACT schema-attr-filter-index (db-schema-v1-design.md §2.12/§3.1 rev 3-4).
-- Author the declared-attribute filter index: okg.attr_index plus the typed
-- partial indexes the operator pushdown reads. JSONB stays the truth; this is
-- a DERIVED projection of the attributes the ontology declares `filterable`.
--
-- Declaring an attribute filterable is CATALOG DATA (a `filterable_attrs` key
-- in okg.subtypes.metadata, compiled from LinkML annotations) and executes NO
-- DDL. This migration authors the fixed table those declarations project into;
-- it is run once, not per declaration. Asserted by
-- tests/substrate/db/test_attr_filter_index.py::
-- test_declaring_filterable_executes_zero_ddl, which arms a DDL-capturing
-- event trigger around a declaration and requires the captured list to be
-- empty.
--
-- Non-destructive and additive: one new table, one new function, six new
-- indexes, two grants. No existing relation is altered. A fresh database gets
-- the same shape from the runtime schema ensure
-- (src/okg/substrate/attributes.py replays this DDL verbatim), so re-running
-- is a no-op.
--
-- The table starts EMPTY. It is populated either by the first full-scope
-- publish derivation or by the rebuild verb
-- (`okg db rebuild-attr-index --apply`); until a branch has rows, the
-- derivation gate reports `bootstrap` rather than reading an empty index as
-- agreement.

-- The only guarded cast in the derivation. `num`/`text`/`bool` admit on jsonb
-- type alone and cannot fail; `ts` is carried as a JSON string of which only
-- some values are timestamps. PARALLEL UNSAFE is required, not conservative:
-- the EXCEPTION block opens a subtransaction, which a parallel worker cannot
-- do ("cannot start commands during a parallel operation"). The exception list
-- is narrow on purpose -- a malformed literal is data the index legitimately
-- skips, but a cancel or an out-of-memory is not, and swallowing those would
-- silently under-populate the index.
CREATE OR REPLACE FUNCTION okg.attr_index_try_ts(v text)
RETURNS timestamptz
LANGUAGE plpgsql IMMUTABLE PARALLEL UNSAFE
AS $fn$
BEGIN
  RETURN v::timestamptz;
EXCEPTION
  WHEN invalid_datetime_format
    OR datetime_field_overflow
    OR invalid_text_representation THEN
    RETURN NULL;
END
$fn$;

-- RUN-COALESCING is the table's defining contract: a row spans a VALUE RUN, so
-- [from_ord, to_ord) covers every interval over which the declared attribute
-- held the same value. Phase-2 measured 15.3% row survival against the naive
-- per-interval projection (0.9 GB vs 5.9 GB at 17M-row scale) and ~6x less
-- publish write volume.
CREATE TABLE IF NOT EXISTS okg.attr_index (
  attr_id TEXT NOT NULL,
  branch_id TEXT NOT NULL,
  namespace TEXT NOT NULL DEFAULT 'default',
  target_kind TEXT NOT NULL
    CHECK (target_kind IN ('node', 'edge')),
  target_id TEXT NOT NULL,
  from_ord BIGINT NOT NULL,
  to_ord BIGINT NOT NULL DEFAULT 9223372036854775807,
  value_num NUMERIC,
  value_text TEXT,
  value_ts TIMESTAMPTZ,
  value_bool BOOLEAN,
  CHECK (from_ord < to_ord),
  -- Exactly one typed column per row, chosen by the declared value_type. This
  -- is what keeps each index run single-typed and range-scannable.
  CONSTRAINT attr_index_exactly_one_typed_value
    CHECK (num_nonnulls(value_num, value_text, value_ts, value_bool) = 1),
  PRIMARY KEY
    (attr_id, branch_id, namespace, target_kind, target_id, from_ord)
);

-- Typed partial indexes on the OPEN front, led by attr_id.
--
-- Two prototype-proven planner rules are baked in here and in the pushdown:
--  (a) the partial predicate names `value_<type> IS NOT NULL`, so an emitted
--      declared-key query must imply that clause for the planner to prove the
--      index applies. A comparison implies it on its own; the ORDERING join
--      does not, and there the explicit clause is what keeps the plan off a
--      sort-over-scan. The pushdown emits it unconditionally;
--  (b) the TEXT index uses the PLAIN btree opclass, deliberately not
--      text_pattern_ops: deployments are C-collation, where the plain opclass
--      serves equality, prefix, AND ORDER BY, and text_pattern_ops cannot
--      serve ORDER BY at all.
CREATE INDEX IF NOT EXISTS attr_index_open_num_idx
  ON okg.attr_index (attr_id, branch_id, namespace, value_num, target_id)
  WHERE to_ord = 9223372036854775807 AND value_num IS NOT NULL;

CREATE INDEX IF NOT EXISTS attr_index_open_text_idx
  ON okg.attr_index (attr_id, branch_id, namespace, value_text, target_id)
  WHERE to_ord = 9223372036854775807 AND value_text IS NOT NULL;

CREATE INDEX IF NOT EXISTS attr_index_open_ts_idx
  ON okg.attr_index (attr_id, branch_id, namespace, value_ts, target_id)
  WHERE to_ord = 9223372036854775807 AND value_ts IS NOT NULL;

CREATE INDEX IF NOT EXISTS attr_index_open_bool_idx
  ON okg.attr_index (attr_id, branch_id, namespace, value_bool, target_id)
  WHERE to_ord = 9223372036854775807 AND value_bool IS NOT NULL;

-- History-ordered variant for as-of-G reads: stab the value run rather than
-- riding the open front.
CREATE INDEX IF NOT EXISTS attr_index_history_idx
  ON okg.attr_index (attr_id, branch_id, namespace, from_ord, to_ord, target_id);

-- Target-led access on the open front. Two consumers: the publish derivation
-- (fetch a dirty target's open rows to decide keep/close/open) and the
-- pushdown's target-led semi-join -- planner rule (c), the measured strategy
-- for broad-AND-broad predicate pairs.
CREATE INDEX IF NOT EXISTS attr_index_target_open_idx
  ON okg.attr_index (branch_id, namespace, target_kind, target_id, attr_id)
  WHERE to_ord = 9223372036854775807;

-- The table churns every publish (close + open on value change), so the
-- cluster-default 20% scale factor would let dead tuples accumulate before
-- autovacuum fires. Matches the sibling attribute tables' tuning.
ALTER TABLE okg.attr_index SET (
  autovacuum_vacuum_scale_factor = 0.05,
  autovacuum_analyze_scale_factor = 0.05,
  autovacuum_vacuum_cost_limit = 2000
);

-- The operator pushdown reads attr_index on the constrained okg_mcp pool, and
-- the registry naming the declared keys lives in the ontology catalog. Both
-- are on the read hot path. Role-guarded and best-effort, mirroring the
-- existing attribute-plane grants.
DO $okg_mcp_attr_index_grants$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'okg_mcp') THEN
    RETURN;
  END IF;
  BEGIN
    GRANT SELECT ON TABLE okg.attr_index TO okg_mcp;
  EXCEPTION WHEN insufficient_privilege OR undefined_table THEN NULL;
  END;
  BEGIN
    GRANT SELECT ON TABLE okg.subtypes TO okg_mcp;
  EXCEPTION WHEN insufficient_privilege OR undefined_table THEN NULL;
  END;
  BEGIN
    GRANT SELECT ON TABLE okg.catalog_versions TO okg_mcp;
  EXCEPTION WHEN insufficient_privilege OR undefined_table THEN NULL;
  END;
END
$okg_mcp_attr_index_grants$;
