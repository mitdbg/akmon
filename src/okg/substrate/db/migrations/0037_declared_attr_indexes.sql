SET LOCAL lock_timeout = '10s';
SET LOCAL statement_timeout = '30min';

-- okg#3112 step 2, slice 1 (PACT lane-h-3112-step2; design
-- pact/changes/lane-h-3112-step2/design.md sections 2.1 and 4.2).
--
-- One partial expression index per declared filterable attribute on each
-- Chronos graph table will replace okg.attr_index. This migration authors
-- only what those indexes and their maintenance need; it builds no index.
-- The reconciler (src/okg/substrate/declared_attr_indexes.py) builds them
-- with CREATE INDEX CONCURRENTLY outside any transaction, on its own queue,
-- after an operator's first `okg db attr-index reconcile --apply`.
--
-- Additive and non-destructive: three functions, three tables, one grant.
-- Re-running is a no-op.

-- The index key of a declared `num` attribute: the value clamped to +-1e300
-- and rounded to 30 decimals. Monotone (non-decreasing) and bounded in size,
-- so no admitted JSON number can exceed the btree row limit.
CREATE OR REPLACE FUNCTION okg.attr_num_key(v numeric)
RETURNS numeric
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
AS $fn$
  SELECT round(greatest(least(v, 1e300), -1e300), 30)
$fn$;

-- The strict ISO-8601 reading of a declared `ts` attribute (v4): 1-9
-- fractional digits, T, t or one space, Z, z or a numeric offset. Read in
-- UTC with a fixed DateStyle, so the result does not depend on the session.
-- After the regex only ASCII digits, separators, T/t and Z/z can remain; a
-- trailing Z or z becomes +00:00, so no time zone abbreviation is ever
-- looked up and timezone_abbreviations cannot change the result. (Setting
-- timezone_abbreviations on the function instead re-reads its file on
-- every call, about 15 times the cost of the whole function.) upper() then
-- only turns t into T.
CREATE OR REPLACE FUNCTION okg.attr_ts_v4(v text)
RETURNS timestamptz
LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
SET timezone = 'UTC'
SET datestyle = 'ISO, YMD'
AS $fn$
DECLARE
  iso text;
BEGIN
  IF v !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}([Tt ][0-9]{2}:[0-9]{2}(:[0-9]{2}(\.[0-9]{1,9})?)?([Zz]|[+-][0-9]{2}(:?[0-9]{2})?)?)?$' THEN
    RETURN NULL;
  END IF;
  iso := upper(v);
  IF right(iso, 1) = 'Z' THEN
    iso := left(iso, -1) || '+00:00';
  END IF;
  IF NOT pg_input_is_valid(iso, 'timestamp with time zone') THEN
    RETURN NULL;
  END IF;
  RETURN iso::timestamptz;
END
$fn$;

-- The double-precision reading of a stored JSON number for the exact filter
-- (never an index): equals v::float8 wherever that cast succeeds, and
-- +-Infinity or +-0 where it would raise 22003, which is what Python's
-- float() gives for the same text.
CREATE OR REPLACE FUNCTION okg.attr_num_f8(v numeric)
RETURNS float8
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
AS $fn$
  SELECT CASE
    WHEN v = 0 OR (abs(v) < 1e308 AND abs(v) > 1e-307) THEN v::float8
    WHEN pg_input_is_valid(v::text, 'float8') THEN v::float8
    WHEN abs(v) >= 1 THEN
      CASE WHEN v > 0 THEN 'Infinity'::float8 ELSE '-Infinity'::float8 END
    ELSE CASE WHEN v > 0 THEN 0::float8 ELSE '-0'::float8 END
  END
$fn$;

-- The manifest: one row per declared index the reconciler owns. Index names
-- carry the reserved prefix okg_dattr_; the reconciler reports any index with
-- that prefix this table does not list.
CREATE TABLE IF NOT EXISTS okg.declared_attr_indexes (
  index_name TEXT PRIMARY KEY,
  target_kind TEXT NOT NULL CHECK (target_kind IN ('node', 'edge')),
  attr_id TEXT NOT NULL,
  value_type TEXT NOT NULL CHECK (value_type IN ('num', 'text', 'ts', 'bool')),
  key_version BIGINT NOT NULL,
  physical_table TEXT NOT NULL,
  definition_digest TEXT NOT NULL,
  state TEXT NOT NULL
    CHECK (state IN ('pending', 'building', 'ready', 'failed', 'drop_failed', 'retiring')),
  attempts BIGINT NOT NULL DEFAULT 0,
  next_attempt_run BIGINT,
  backend_pid BIGINT,
  state_since TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_error TEXT,
  last_reason TEXT,
  indexdef TEXT,
  max_value_chars BIGINT,
  drift BOOLEAN NOT NULL DEFAULT false,
  drift_detected_at TIMESTAMPTZ,
  drift_detail JSONB,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT declared_attr_indexes_name_prefix
    CHECK (index_name LIKE 'okg\_dattr\_%')
);

-- At most one live index per (kind, attribute); a retiring one may coexist
-- with its replacement until it is dropped.
CREATE UNIQUE INDEX IF NOT EXISTS declared_attr_indexes_live_key
  ON okg.declared_attr_indexes (target_kind, attr_id)
  WHERE state IN ('pending', 'building', 'ready', 'failed');

-- Reconciler run state (one row): the run counter the retry backoff counts
-- in, the pid and backend start of the session that holds the index-DDL lock
-- for a run (so a migrate or catalog apply can cancel it, and never a later
-- session that reused the pid), the operator's opt-in, maintenance
-- requests, and the drift-check cursor.
CREATE TABLE IF NOT EXISTS okg.declared_attr_index_state (
  singleton BOOLEAN PRIMARY KEY DEFAULT true CHECK (singleton),
  enabled_at TIMESTAMPTZ,
  run_count BIGINT NOT NULL DEFAULT 0,
  reconciler_pid BIGINT,
  reconciler_backend_start TIMESTAMPTZ,
  last_run_started_at TIMESTAMPTZ,
  last_run_finished_at TIMESTAMPTZ,
  last_run_report JSONB,
  requested_at TIMESTAMPTZ,
  request_reason TEXT,
  drift_checked_through_ordinal BIGINT,
  slice_count BIGINT,
  slice_cursor BIGINT NOT NULL DEFAULT 0,
  slice_boundaries JSONB,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One row per drift check: the metric doctor reads.
CREATE TABLE IF NOT EXISTS okg.declared_attr_index_drift_checks (
  check_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  check_kind TEXT NOT NULL CHECK (check_kind IN ('dirty', 'slice', 'full')),
  branch_name TEXT NOT NULL,
  ordinal BIGINT NOT NULL,
  slice_index BIGINT,
  slice_count BIGINT,
  targets_checked BIGINT NOT NULL,
  differences BIGINT NOT NULL,
  by_attr JSONB NOT NULL DEFAULT '{}'::jsonb,
  sample JSONB NOT NULL DEFAULT '[]'::jsonb,
  checked_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS declared_attr_index_drift_checks_recent
  ON okg.declared_attr_index_drift_checks (checked_at DESC);

-- The reader slice reads the manifest's readiness on the okg_mcp pool.
DO $okg_mcp_declared_attr_index_grants$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'okg_mcp') THEN
    RETURN;
  END IF;
  BEGIN
    GRANT SELECT ON TABLE okg.declared_attr_indexes TO okg_mcp;
  EXCEPTION WHEN insufficient_privilege OR undefined_table THEN NULL;
  END;
END
$okg_mcp_declared_attr_index_grants$;
