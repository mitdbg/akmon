-- Executed by the multi-phase driver's ensure_infra batch: SET LOCAL scopes
-- both timeouts to that batch's transaction.
SET LOCAL lock_timeout = '10s';
SET LOCAL statement_timeout = '30min';

-- PACT schema-interval-narrow-split (db-schema-v1-design.md §2.12/§3.1 rev 4),
-- rephased by PACT phase-interval-narrow-split-migration (issues #1143/#1114).
--
-- Split the attrs interval representation: jsonb payloads move to an
-- insert-once content-addressed store (okg.attribute_versions, PK
-- (target_kind, target_id, attrs_hash)); the interval and snapshot tables keep
-- their real keys and filter columns and carry a bytea(32) attrs_hash
-- reference under a composite FK ON DELETE RESTRICT. The hash is computed at
-- exactly ONE site, in-database, over the deterministic rendering of the
-- STORED jsonb value (okg.attribute_content_hash) — never a Python-side
-- serialization. presence-means-overlay NULL semantics for raw_attrs are
-- preserved in the version store (NULL means raw == attrs; never DEFAULT '{}').
--
-- class: multi_phase — this file is NOT executed by the ordered runner. Its
-- original single-transaction DO block rewrote every row of both wide tables,
-- copied the full jsonb payload under per-row ON CONFLICT probes, and
-- validated two composite FKs at ADD time, all atomically: measured 1.35 TB
-- of reads and 583 GB of writes against a 12 GB working set without
-- completing (issue #1143). The data motion now runs through the
-- checkpointed, resumable multi-phase driver
-- (okg.substrate.db.multiphase.run_multiphase_migration) with the phase plan
-- registered in okg.substrate.db.interval_narrow_split_migration: batched
-- keyset backfill (hash computed once per row, version-store inserts in PK
-- order), an ADD CHECK ... NOT VALID writer fence, a bounded straggler
-- sweep, VALIDATE in its own lock scope, metadata-only SET NOT NULL via the
-- validated check, FKs added NOT VALID and validated as a separate resumable
-- phase, and metadata-only column drops. Every phase commits bounded batches
-- with a durable watermark in okg.schema_migration_phases, so a kill at any
-- point leaves a labelled, resumable state.
--
-- The CREATE statements below are the migration's schema-shape DDL. The
-- driver's ensure_infra phase parses THIS file and executes them (single
-- source — the Python spec carries no duplicate DDL text). The function and
-- table text is the SAME text src/okg/substrate/attributes.py replays on
-- every schema ensure. Idempotent: CREATE OR REPLACE / IF NOT EXISTS, and a
-- fresh database where the runtime ensures create the narrow shape directly
-- leaves every phase a guarded no-op.

CREATE OR REPLACE FUNCTION okg.jsonb_canonical_text(j jsonb)
RETURNS text
LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE
AS $fn$
DECLARE
  result text;
BEGIN
  -- Scalar leaves are rendered inline (jsonb's own text output is canonical
  -- for strings/booleans/null; numbers are trim_scale-normalized so
  -- jsonb-EQUAL numerics of different scale render identically); recursion
  -- happens only for nested containers. Keys sort under COLLATE "C" for a
  -- collation-independent rendering.
  CASE jsonb_typeof(j)
    WHEN 'object' THEN
      SELECT '{' || COALESCE(string_agg(
               to_jsonb(e.key)::text || ':' ||
               CASE jsonb_typeof(e.value)
                 WHEN 'object' THEN okg.jsonb_canonical_text(e.value)
                 WHEN 'array' THEN okg.jsonb_canonical_text(e.value)
                 WHEN 'number' THEN
                   trim_scale((e.value #>> '{}')::numeric)::text
                 ELSE e.value::text
               END,
               ',' ORDER BY e.key COLLATE "C"), '') || '}'
        INTO result
        FROM jsonb_each(j) AS e(key, value);
    WHEN 'array' THEN
      SELECT '[' || COALESCE(string_agg(
               CASE jsonb_typeof(a.value)
                 WHEN 'object' THEN okg.jsonb_canonical_text(a.value)
                 WHEN 'array' THEN okg.jsonb_canonical_text(a.value)
                 WHEN 'number' THEN
                   trim_scale((a.value #>> '{}')::numeric)::text
                 ELSE a.value::text
               END, ',' ORDER BY a.ord), '') || ']'
        INTO result
        FROM jsonb_array_elements(j) WITH ORDINALITY AS a(value, ord);
    WHEN 'number' THEN
      result := trim_scale((j #>> '{}')::numeric)::text;
    ELSE
      result := j::text;
  END CASE;
  RETURN result;
END
$fn$;

CREATE OR REPLACE FUNCTION okg.attribute_content_hash(attrs jsonb, raw_attrs jsonb)
RETURNS bytea
LANGUAGE sql IMMUTABLE PARALLEL SAFE
AS $fn$
  SELECT sha256(convert_to(
    okg.jsonb_canonical_text(attrs)
    || E'\x1f'
    || CASE WHEN raw_attrs IS NULL THEN E'\x1e'
            ELSE okg.jsonb_canonical_text(raw_attrs) END,
    'UTF8'))
$fn$;

CREATE TABLE IF NOT EXISTS okg.attribute_versions (
  target_kind TEXT NOT NULL
    CHECK (target_kind IN ('node', 'edge')),
  target_id TEXT NOT NULL,
  attrs_hash BYTEA NOT NULL,
  attrs JSONB NOT NULL,
  raw_attrs JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (target_kind, target_id, attrs_hash)
);

-- Data motion (the wide -> narrow convergence of okg.attribute_effective_view
-- and okg.attribute_effective_view_intervals) is DELIBERATELY absent from
-- this file: it runs as checkpointed batches through the registered
-- multi-phase spec (okg.substrate.db.interval_narrow_split_migration), never
-- inside one transaction. The runtime schema ensure refuses a populated wide
-- database instead of converging it inline (src/okg/substrate/attributes.py),
-- so this spec is the ONLY owner of the wide-shape data motion.
