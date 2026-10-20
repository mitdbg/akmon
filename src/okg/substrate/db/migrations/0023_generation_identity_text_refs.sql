-- schema-identity-convergence / D2 (docs/architecture/db-schema-v1-design.md
-- §3.2): the TEXT checkpoint ref is the sole generation reference in the
-- granted surface. This migration converges an upgraded database onto the
-- v1.0 shape that schema.sql now authors directly.
--
-- Scope: only columns whose every consumer uses EQUALITY semantics. The
-- derived/consistency/lens `target_generation_id` family is consumed as an
-- ORDER (<=, <, max(), ORDER BY ... DESC) at 40+ call sites and is NOT
-- converted here; see the deferred-divergence registry in
-- src/okg/substrate/db/identity_divergences.yaml.
--
-- Rebuild note: the `USING col::text` casts render the legacy numeric compat
-- token as its decimal string. Those strings sort before real Chronos refs
-- ('okg:...'), which is harmless for the equality-only consumers converted
-- here, and is regenerated wholesale by the v1.0 rebuild.

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

-- 1. okg.admission_envelopes: drop the legacy numeric compat token entirely.
--    okg.v_admission_envelopes projects it, so the view is rebuilt without it
--    (views.sql re-creates it identically on the next migrate).
DROP VIEW IF EXISTS okg.v_admission_envelopes;

DROP INDEX IF EXISTS okg.admission_envelopes_generation_idx;

--    BACKFILL BEFORE THE DROP. Every producer path writes
--    target_generation_ref today, but envelopes published before that column
--    existed carry their generation ONLY in the numeric token. Dropping the
--    column without this UPDATE would silently discard the sole generation
--    linkage of those rows --- the exact silent data loss this change fixes
--    elsewhere. Guarded on the column's existence because a v1.0 fresh install
--    never had it, and an unguarded UPDATE would fail to parse there.
DO $backfill_admission_generation_ref$
BEGIN
  IF EXISTS (
    SELECT 1
      FROM pg_attribute
     WHERE attrelid = 'okg.admission_envelopes'::regclass
       AND attname = 'target_generation_id'
       AND attnum > 0
       AND NOT attisdropped
  ) THEN
    EXECUTE $backfill$
      UPDATE okg.admission_envelopes
         SET target_generation_ref = target_generation_id::text
       WHERE target_generation_ref IS NULL
         AND target_generation_id IS NOT NULL
    $backfill$;
  END IF;
END
$backfill_admission_generation_ref$;

ALTER TABLE okg.admission_envelopes
  DROP COLUMN IF EXISTS target_generation_id;

ALTER TABLE okg.admission_envelopes
  ALTER COLUMN target_generation_ref TYPE TEXT COLLATE "C";

-- 2. okg.llm_calls: the join partner of okg.work_ledger.generation_id (TEXT).
--    While this column was BIGINT the writer coerced every Chronos checkpoint
--    ref to NULL, so the documented cost join silently returned zero spend.
ALTER TABLE okg.llm_calls
  ALTER COLUMN generation_id TYPE TEXT COLLATE "C"
  USING generation_id::text;

--    Both sides of the cost join carry the same collation, so the comparison
--    is a memcmp with abbreviated keys rather than a locale-aware collate.
--    okg.work_ledger's PK covers this column, so the retype rebuilds it.
ALTER TABLE okg.work_ledger
  ALTER COLUMN generation_id TYPE TEXT COLLATE "C";

-- 3. okg.ir_program_bindings / okg.ir_invocations. The bindings dedupe index
--    is an expression index over the column, so it must be dropped before the
--    retype and re-created against the TEXT sentinel ('' rather than -1).
DROP VIEW IF EXISTS okg.v_ir_program_bindings;
DROP VIEW IF EXISTS okg.v_ir_invocations;

DROP INDEX IF EXISTS okg.ir_program_bindings_dedupe_idx;
DROP INDEX IF EXISTS okg.ir_invocations_pin_idx;

ALTER TABLE okg.ir_program_bindings
  ALTER COLUMN generation_id TYPE TEXT COLLATE "C"
  USING generation_id::text;

ALTER TABLE okg.ir_invocations
  ALTER COLUMN generation_id TYPE TEXT COLLATE "C"
  USING generation_id::text;

CREATE UNIQUE INDEX IF NOT EXISTS ir_program_bindings_dedupe_idx
  ON okg.ir_program_bindings (
    program_id,
    deployment_name,
    owner_kind,
    owner_id,
    program_role,
    COALESCE(generation_id::text, ''),
    COALESCE(branch_name, '')
  );

CREATE INDEX IF NOT EXISTS ir_invocations_pin_idx
  ON okg.ir_invocations (
    deployment_name, branch_name, generation_id, invocation_kind
  );

--    The dedupe index collapses NULL to ''. That is sound only while no real
--    generation ref can BE the empty string, so the rule is enforced rather
--    than assumed. Born VALID: after the ::text cast above every existing
--    value is either NULL or a run of digits.
ALTER TABLE okg.ir_program_bindings
  DROP CONSTRAINT IF EXISTS ir_program_bindings_generation_id_nonempty_chk;
ALTER TABLE okg.ir_program_bindings
  ADD CONSTRAINT ir_program_bindings_generation_id_nonempty_chk
  CHECK (generation_id IS NULL OR length(btrim(generation_id::text)) > 0);

-- 4. Nomos bundle/config provenance. Both reach the okg_mcp granted surface
--    through okg.v_nomos_policy_bundles / okg.v_nomos_config_sets.
DROP VIEW IF EXISTS okg.v_nomos_policy_bundles;
DROP VIEW IF EXISTS okg.v_nomos_config_sets;

ALTER TABLE okg.nomos_policy_bundles
  ALTER COLUMN source_generation_id TYPE TEXT COLLATE "C"
  USING source_generation_id::text;

ALTER TABLE okg.nomos_config_sets
  ALTER COLUMN source_generation_id TYPE TEXT COLLATE "C"
  USING source_generation_id::text;

-- 5. Re-create every view this migration dropped.
--
-- Required by scripts/check_migration_drop_recreate.py (issue #1054): a
-- migration that drops a relation the shipped tree re-creates elsewhere leaves
-- a postcondition that is true only until the next schema apply. Re-creating
-- them here makes the migration self-contained --- when it commits, the
-- database is in the shape it claims. views.sql re-issues the identical
-- definitions later in the same migrate, which is then a no-op refresh.
--
-- These definitions are copied from src/okg/substrate/db/views.sql. Drift is
-- caught loudly rather than silently: views.sql runs afterwards with CREATE OR
-- REPLACE, which refuses a changed column name or order, and the
-- migration-convergence lane compares the final shapes.

CREATE OR REPLACE VIEW okg.v_admission_envelopes AS
SELECT
  admission_id::text AS admission_id,
  deployment_name,
  producer_id,
  producer_kind,
  intent,
  admission_mode,
  status,
  catalog_version_id,
  source,
  run_id::text AS run_id,
  idempotency_key,
  authority,
  check_summary,
  verdict,
  workflow_ref,
  created_at,
  updated_at,
  decided_at,
  published_at,
  target_generation_ref,
  NULLIF(btrim(target_generation_ref), '') AS canonical_generation_ref
FROM okg.admission_envelopes;

CREATE OR REPLACE VIEW okg.v_ir_program_bindings AS
SELECT
  binding_id,
  program_id,
  deployment_name,
  owner_kind,
  owner_id,
  program_role,
  generation_id,
  branch_name,
  metadata,
  created_at
FROM okg.ir_program_bindings;

CREATE OR REPLACE VIEW okg.v_ir_invocations AS
SELECT
  invocation_id,
  program_id,
  binding_id,
  deployment_name,
  invocation_kind,
  branch_name,
  generation_id,
  status,
  verdict,
  reason,
  started_at,
  completed_at,
  result_count,
  node_delta_count,
  edge_delta_count,
  cost_proxy,
  quality_score,
  error_count,
  truncation_count,
  metrics,
  created_at
FROM okg.ir_invocations;

CREATE OR REPLACE VIEW okg.v_nomos_policy_bundles AS
SELECT
  bundle_row_id,
  deployment_name,
  policy_bundle_id,
  policy_version,
  policy_fingerprint,
  compiler_version,
  source_generation_id,
  scope,
  default_effect,
  reason_code_catalog,
  bundle,
  status,
  effective_from,
  effective_until,
  compiled_at,
  metadata,
  created_at,
  updated_at
FROM okg.nomos_policy_bundles;

CREATE OR REPLACE VIEW okg.v_nomos_config_sets AS
SELECT
  c.config_row_id,
  c.deployment_name,
  c.config_set_id,
  c.config_version,
  c.config_fingerprint,
  c.compiler_version,
  c.source_generation_id,
  c.scope,
  redacted.entries AS entries,
  CASE
    WHEN jsonb_typeof(c.config_set) = 'object' THEN
      (c.config_set - 'entries') ||
      jsonb_build_object('entries', redacted.entries)
    ELSE jsonb_build_object('entries', redacted.entries)
  END AS config_set,
  c.status,
  c.effective_from,
  c.effective_until,
  c.compiled_at,
  CASE
    WHEN c.metadata = '{}'::jsonb THEN '{}'::jsonb
    ELSE jsonb_build_object('metadata_redacted', true)
  END AS metadata,
  c.created_at,
  c.updated_at
FROM okg.nomos_config_sets c
CROSS JOIN LATERAL (
  SELECT COALESCE(
    jsonb_agg(
      jsonb_build_object(
        'config_key', entry->'config_key',
        'classification', entry->'classification',
        'scope', entry->'scope',
        'owner', entry->'owner',
        'metadata_redacted',
          (entry ? 'metadata') AND (entry->'metadata' <> '{}'::jsonb),
        'value_redacted', entry ? 'value',
        'secret_ref_redacted', entry ? 'secret_ref'
      )
      ORDER BY ord
    ),
    '[]'::jsonb
  ) AS entries
  FROM jsonb_array_elements(c.entries) WITH ORDINALITY AS item(entry, ord)
) redacted;
