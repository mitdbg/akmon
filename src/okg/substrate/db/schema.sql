-- OKG substrate v1 schema.
--
-- Every DDL statement is idempotent: running this file against an
-- already-migrated DB is a no-op. The migrate runner (okg migrate)
-- splits on ';' and executes each statement, logging progress.
--
-- Authoritative references:
--   - design.md "Data model" + "ERD" sections
--   - docs/develop/architecture/pipeline.md#publication-result-types
--   - docs/develop/architecture/overview.md (publication service)

-- ========== Schema + extensions ==========

CREATE SCHEMA IF NOT EXISTS okg;
CREATE SCHEMA IF NOT EXISTS okg_derived;

-- Extensions are installed by ops/pg/initdb/00-extensions.sql on a
-- fresh compose. For a managed host, okg migrate asserts they are
-- installed and refuses if any are missing (okg.substrate.db.doctor).

-- ========== Catalog versioning ==========

CREATE TABLE IF NOT EXISTS okg.catalog_versions (
  catalog_version_id  BIGSERIAL PRIMARY KEY,
  status              TEXT NOT NULL
    CHECK (status IN ('pending','applied','rolled_back')),
  applied_at          TIMESTAMPTZ,
  diff_summary        JSONB NOT NULL DEFAULT '{}'::jsonb,
  reconcile_plan      JSONB NOT NULL DEFAULT '{}'::jsonb,
  deployment_name     TEXT,
  catalog_fingerprint TEXT,
  schema_fingerprint  TEXT,
  writer_fingerprint  TEXT,
  writer_provenance   JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_by_command  TEXT,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS catalog_versions_status_idx
  ON okg.catalog_versions (status);

ALTER TABLE okg.catalog_versions
  ADD COLUMN IF NOT EXISTS deployment_name TEXT;
ALTER TABLE okg.catalog_versions
  ADD COLUMN IF NOT EXISTS catalog_fingerprint TEXT;
ALTER TABLE okg.catalog_versions
  ADD COLUMN IF NOT EXISTS schema_fingerprint TEXT;
ALTER TABLE okg.catalog_versions
  ADD COLUMN IF NOT EXISTS writer_fingerprint TEXT;
ALTER TABLE okg.catalog_versions
  ADD COLUMN IF NOT EXISTS writer_provenance JSONB NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE okg.catalog_versions
  ADD COLUMN IF NOT EXISTS created_by_command TEXT;

CREATE INDEX IF NOT EXISTS catalog_versions_deployment_fingerprint_idx
  ON okg.catalog_versions
  (deployment_name, status, catalog_fingerprint, catalog_version_id DESC);

CREATE TABLE IF NOT EXISTS okg.deployment_write_ownership (
  deployment_name       TEXT PRIMARY KEY,
  owner_fingerprint     TEXT NOT NULL,
  schema_fingerprint    TEXT NOT NULL,
  catalog_fingerprint   TEXT NOT NULL,
  deployment_root_hash  TEXT,
  worktree_root_hash    TEXT,
  status                TEXT NOT NULL DEFAULT 'active'
    CHECK (status IN ('active','released')),
  claimed_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
  claimed_by_command    TEXT,
  last_seen_catalog_version_id BIGINT,
  -- #709 bind-deployment-identity: manifest content-hash over the
  -- publisher-stack + source-sync input closure. NULL means the claim
  -- predates this feature (warns + doctor-flags at publish, backfilled
  -- only by claim-family commands); per-block hashes in metadata.
  manifest_fingerprint  TEXT,
  metadata              JSONB NOT NULL DEFAULT '{}'::jsonb
);
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
      FROM information_schema.columns
     WHERE table_schema = 'okg'
       AND table_name = 'deployment_write_ownership'
       AND column_name = 'manifest_fingerprint'
  ) THEN
    ALTER TABLE okg.deployment_write_ownership
      ADD COLUMN manifest_fingerprint TEXT;
  END IF;
END
$$;

-- ========== Deployment promotion fence ==========
-- A production promotion owns this row from the first writer fence until
-- either production or the previous release has passed readiness. Enqueue
-- producers serialize against the same advisory lock used to create/delete
-- the row, so no new DBOS work can cross the fence-drain boundary.
CREATE TABLE IF NOT EXISTS okg.deployment_promotion_fences (
  deployment_name       TEXT PRIMARY KEY,
  promotion_id          TEXT NOT NULL,
  status                TEXT NOT NULL DEFAULT 'fenced'
    CHECK (status IN ('fenced', 'recovery_blocked', 'released')),
  schedules_removed     INTEGER NOT NULL DEFAULT 0
    CHECK (schedules_removed >= 0),
  schedule_snapshot     JSONB NOT NULL DEFAULT '[]'::jsonb,
  fenced_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
  blocker               JSONB,
  metadata              JSONB NOT NULL DEFAULT '{}'::jsonb
);

-- Append-only operator disposition for exact retained DBOS terminal failures.
-- DBOS remains the immutable execution-history authority; these rows only state
-- that a named failure snapshot has an independently validated recovery witness.
CREATE TABLE IF NOT EXISTS okg.runtime_dlq_acknowledgement_events (
  acknowledgement_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  deployment_name          TEXT NOT NULL CHECK (btrim(deployment_name) <> ''),
  workflow_id              TEXT NOT NULL CHECK (btrim(workflow_id) <> ''),
  workflow_name            TEXT NOT NULL CHECK (btrim(workflow_name) <> ''),
  application_version      TEXT NOT NULL CHECK (btrim(application_version) <> ''),
  failure_snapshot_digest  TEXT NOT NULL
    CHECK (failure_snapshot_digest ~ '^sha256:[0-9a-f]{64}$'),
  error_digest             TEXT NOT NULL
    CHECK (error_digest ~ '^sha256:[0-9a-f]{64}$'),
  responsibility           JSONB NOT NULL,
  actor                    TEXT NOT NULL CHECK (btrim(actor) <> ''),
  reason                   TEXT NOT NULL CHECK (btrim(reason) <> ''),
  candidate_revision       TEXT NOT NULL CHECK (btrim(candidate_revision) <> ''),
  operation_receipt_digest TEXT NOT NULL
    CHECK (operation_receipt_digest ~ '^sha256:[0-9a-f]{64}$'),
  witness                  JSONB NOT NULL,
  plan_payload             JSONB NOT NULL,
  plan_id                  TEXT NOT NULL
    CHECK (plan_id ~ '^sha256:[0-9a-f]{64}$'),
  acknowledged_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (workflow_id)
);

CREATE INDEX IF NOT EXISTS runtime_dlq_ack_events_created_idx
  ON okg.runtime_dlq_acknowledgement_events
  (deployment_name, acknowledged_at DESC);

CREATE OR REPLACE FUNCTION okg.reject_runtime_dlq_acknowledgement_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
BEGIN
  RAISE EXCEPTION 'runtime DLQ acknowledgement events are append-only';
END
$function$;

DROP TRIGGER IF EXISTS runtime_dlq_acknowledgements_immutable
  ON okg.runtime_dlq_acknowledgement_events;
CREATE TRIGGER runtime_dlq_acknowledgements_immutable
  BEFORE UPDATE OR DELETE OR TRUNCATE
  ON okg.runtime_dlq_acknowledgement_events
  FOR EACH STATEMENT
  EXECUTE FUNCTION okg.reject_runtime_dlq_acknowledgement_mutation();

-- ========== Typed DBOS operational outcomes ==========
-- Successful participant workflows persist typed evidence here rather than
-- asking readers to decode dbos.workflow_status.output (which is pickled).
CREATE TABLE IF NOT EXISTS okg.workflow_operational_registrations (
  workflow_uuid    TEXT PRIMARY KEY,
  deployment_name  TEXT NOT NULL CHECK (btrim(deployment_name) <> ''),
  workflow_name    TEXT NOT NULL CHECK (btrim(workflow_name) <> ''),
  queue_name       TEXT NOT NULL CHECK (btrim(queue_name) <> ''),
  contract_version SMALLINT NOT NULL
    CONSTRAINT workflow_operational_registrations_version_check
    CHECK (contract_version = 1),
  registered_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS okg.workflow_operational_outcomes (
  workflow_uuid    TEXT PRIMARY KEY
    REFERENCES okg.workflow_operational_registrations(workflow_uuid),
  contract_version SMALLINT NOT NULL
    CONSTRAINT workflow_operational_outcomes_version_check
    CHECK (contract_version = 1),
  outcome          TEXT NOT NULL
    CONSTRAINT workflow_operational_outcomes_outcome_check
    CHECK (outcome IN ('clean', 'partial_failure', 'retained_failure', 'skipped')),
  succeeded_count  INTEGER NOT NULL CHECK (succeeded_count >= 0),
  failed_count     INTEGER NOT NULL CHECK (failed_count >= 0),
  semantic_units   JSONB NOT NULL CHECK (jsonb_typeof(semantic_units) = 'object'),
  details          JSONB NOT NULL DEFAULT '{}'::jsonb
    CHECK (jsonb_typeof(details) = 'object'),
  recorded_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT workflow_operational_outcomes_counts_check CHECK (
    (outcome = 'clean' AND failed_count = 0)
    OR (outcome = 'partial_failure' AND succeeded_count > 0 AND failed_count > 0)
    OR (outcome = 'retained_failure' AND failed_count > 0)
    OR (outcome = 'skipped' AND succeeded_count = 0 AND failed_count = 0)
  )
);

-- Durable, predecessor-bound journal for crash-safe derived-state recovery.
CREATE TABLE IF NOT EXISTS okg.effective_attribute_recovery_attempts (
  classification_binding_digest TEXT PRIMARY KEY,
  deployment_name TEXT NOT NULL,
  branch_name TEXT NOT NULL,
  predecessor_generation_id TEXT NOT NULL,
  expected_classification JSONB NOT NULL,
  stage TEXT NOT NULL CHECK (
    stage IN ('classified', 'snapshot_committed', 'interval_committed', 'prepared')
  ),
  authorization_payload JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE okg.effective_attribute_recovery_attempts
  ADD COLUMN IF NOT EXISTS consumed_by_generation_id TEXT,
  ADD COLUMN IF NOT EXISTS consumed_at TIMESTAMPTZ;

-- ========== Per-database owner stamp (P0 #419 prevent-recurrence) ==========
-- Exactly ONE active owner identity per physical database. The publish path
-- (Publisher.reserve) asserts that the publishing deployment matches this
-- stamp BEFORE any generation-producing write and FAILS CLOSED on mismatch,
-- so a process configured for deployment X cannot publish into deployment Y's
-- database — the cross-deployment contamination class (issue #419; the
-- 2026-06-10 clone-DB incident). Identity is read from INSIDE the database,
-- independent of the ambient OKG_DSN that selected the connection: the one
-- authority the two disagreeing ambient sources (manifest name, env DSN)
-- never reconcile against. The `singleton` PK + CHECK makes a second active
-- owner a database-layer violation, not a Python convention.
CREATE TABLE IF NOT EXISTS okg.database_owner (
  singleton              BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
  owner_deployment_name  TEXT NOT NULL,
  db_owner_uuid          UUID NOT NULL DEFAULT gen_random_uuid(),
  schema_fingerprint     TEXT,
  stamped_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
  stamped_by_command     TEXT
);

-- ========== Forward migration ledger ==========
--
-- `schema.sql` remains the installable current-state snapshot. Forward
-- and destructive changes are tracked beside it so operators can see drift
-- and pending destructive migrations without replacing the snapshot-based
-- bootstrap path in one step.

CREATE TABLE IF NOT EXISTS okg.schema_migrations (
  migration_id    TEXT PRIMARY KEY,
  checksum        TEXT NOT NULL,
  description     TEXT NOT NULL,
  destructive     BOOLEAN NOT NULL DEFAULT false,
  applied_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  applied_by      TEXT NOT NULL DEFAULT current_user,
  tool_version    TEXT,
  status          TEXT NOT NULL DEFAULT 'applied'
    CHECK (status IN ('applied', 'failed', 'skipped')),
  result_metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

-- Durable progress state for declared multi-phase migrations
-- (`schema-migration-hardening` / req.multiphase-class). One row per
-- (migration, phase); each batch of a multi-phase run commits its data
-- motion and its watermark update in the SAME transaction, so this row
-- is always an exact checkpoint: crash mid-run, resume from watermark.
-- In-progress rows are the doctor-visible mid-state
-- (`multiphase_migration_in_progress`); completion is recorded in
-- okg.schema_migrations with applied_via=multiphase_runner. This table
-- ships in the snapshot ONLY — the multi-phase driver refuses when it
-- is absent rather than ensuring it at runtime.
CREATE TABLE IF NOT EXISTS okg.schema_migration_phases (
  migration_id       TEXT NOT NULL,
  phase              TEXT NOT NULL,
  state              TEXT NOT NULL DEFAULT 'in_progress'
    CHECK (state IN ('in_progress', 'complete')),
  batches_committed  BIGINT NOT NULL DEFAULT 0,
  watermark          JSONB NOT NULL DEFAULT 'null'::jsonb,
  started_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (migration_id, phase)
);

-- Seed the initial catalog version so the first migrate produces a
-- valid FK target for graph_generations and the ontology tables.
-- Idempotent via `ON CONFLICT DO NOTHING` on an anchored id.
INSERT INTO okg.catalog_versions
  (catalog_version_id, status, applied_at, diff_summary, reconcile_plan)
VALUES
  (1, 'applied', now(),
   '{"reason":"initial seed at okg migrate"}'::jsonb,
   '{"reconcile":[]}'::jsonb)
ON CONFLICT (catalog_version_id) DO NOTHING;

-- The sequence now needs to start above the seeded row so future
-- inserts don't collide.
SELECT setval(
  pg_get_serial_sequence('okg.catalog_versions', 'catalog_version_id'),
  GREATEST(1, (SELECT max(catalog_version_id) FROM okg.catalog_versions))
);

-- ========== Ontology catalog (versioned) ==========

CREATE TABLE IF NOT EXISTS okg.subtypes (
  name                TEXT    NOT NULL,
  metadata            JSONB   NOT NULL DEFAULT '{}'::jsonb,
  parent_subtype      TEXT,   -- NULL = root subtype; else single-parent
                              -- refinement per `add-subtype-refines-to-catalog`.
  catalog_version_id  BIGINT  NOT NULL
    REFERENCES okg.catalog_versions(catalog_version_id),
  PRIMARY KEY (name, catalog_version_id),
  -- Parent must exist in the same catalog version. Deferred so the
  -- INSERT pass for a fresh cv can populate subtypes in any order
  -- within a single transaction.
  CONSTRAINT subtypes_parent_fk
    FOREIGN KEY (parent_subtype, catalog_version_id)
    REFERENCES okg.subtypes (name, catalog_version_id)
    DEFERRABLE INITIALLY DEFERRED
);

-- Backfill column for pre-existing deployments. New deployments
-- get the column from the CREATE TABLE above; older deployments
-- whose okg.subtypes was created before the column existed pick
-- it up here. Idempotent.
ALTER TABLE okg.subtypes
  ADD COLUMN IF NOT EXISTS parent_subtype TEXT;

-- The DEFERRABLE FK only exists when the table is created fresh
-- (CREATE TABLE above). For pre-existing deployments where the
-- column is added later, install the FK separately. The
-- pg_constraint lookup avoids a re-add error on already-installed
-- deployments.
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conname = 'subtypes_parent_fk'
       AND conrelid = 'okg.subtypes'::regclass
  ) THEN
    ALTER TABLE okg.subtypes
      ADD CONSTRAINT subtypes_parent_fk
      FOREIGN KEY (parent_subtype, catalog_version_id)
      REFERENCES okg.subtypes (name, catalog_version_id)
      DEFERRABLE INITIALLY DEFERRED;
  END IF;
END$$;

-- Closure table for subtype refinement (reflexive-transitive).
-- Populated by `okg.substrate.catalog.modules_apply` at
-- catalog-compile time. Every subtype has its own depth-0 row;
-- non-root subtypes have depth-N rows for each ancestor in the
-- refinement chain. Realistic chain depth is 1-3.
CREATE TABLE IF NOT EXISTS okg.subtype_ancestors (
  catalog_version_id  BIGINT  NOT NULL
    REFERENCES okg.catalog_versions(catalog_version_id),
  subtype_name        TEXT    NOT NULL,
  ancestor_name       TEXT    NOT NULL,
  depth               INT     NOT NULL,
  PRIMARY KEY (catalog_version_id, subtype_name, ancestor_name)
);

CREATE INDEX IF NOT EXISTS subtype_ancestors_by_ancestor
  ON okg.subtype_ancestors (catalog_version_id, ancestor_name);

-- Backfill depth-0 reflexive rows for every pre-existing subtype
-- (those created before this column landed). New
-- catalog-version compiles handle their own closure population
-- via `okg.substrate.catalog.modules_apply`. Idempotent.
INSERT INTO okg.subtype_ancestors
  (catalog_version_id, subtype_name, ancestor_name, depth)
SELECT s.catalog_version_id, s.name, s.name, 0
  FROM okg.subtypes s
 WHERE NOT EXISTS (
   SELECT 1 FROM okg.subtype_ancestors sa
    WHERE sa.catalog_version_id = s.catalog_version_id
      AND sa.subtype_name = s.name
      AND sa.ancestor_name = s.name
 );

CREATE TABLE IF NOT EXISTS okg.narrowings (
  parent_subtype      TEXT    NOT NULL,
  edge_type           TEXT    NOT NULL,
  child_subtype       TEXT    NOT NULL,
  attr_rules          JSONB   NOT NULL DEFAULT '{}'::jsonb,
  catalog_version_id  BIGINT  NOT NULL
    REFERENCES okg.catalog_versions(catalog_version_id),
  PRIMARY KEY (parent_subtype, edge_type, child_subtype, catalog_version_id)
);

CREATE TABLE IF NOT EXISTS okg.aliases (
  alias               TEXT    NOT NULL,
  normalized_alias    TEXT,
  canonical           TEXT    NOT NULL,
  scope               TEXT,    -- NULL = global; else subtype name
  catalog_version_id  BIGINT  NOT NULL
    REFERENCES okg.catalog_versions(catalog_version_id),
  PRIMARY KEY (alias, catalog_version_id)
);

ALTER TABLE okg.aliases
  ADD COLUMN IF NOT EXISTS normalized_alias TEXT;
CREATE INDEX IF NOT EXISTS aliases_normalized_lookup_idx
  ON okg.aliases (catalog_version_id, scope, normalized_alias)
  WHERE normalized_alias IS NOT NULL;

-- Atomic identity ownership for extracted nodes (#796).  The primary key is
-- deliberately independent of policy version: changing policy must not mint a
-- second canonical for an already-claimed normalized identity.
CREATE TABLE IF NOT EXISTS okg.entity_identity_claims (
  deployment_name     TEXT NOT NULL,
  branch_name         TEXT NOT NULL,
  subtype             TEXT NOT NULL,
  normalized_key      TEXT NOT NULL,
  canonical_node_id   TEXT NOT NULL,
  raw_name            TEXT NOT NULL,
  resolution_method   TEXT NOT NULL,
  policy_fingerprint  TEXT NOT NULL,
  catalog_version_id  BIGINT,
  graph_token         TEXT NOT NULL,
  claimed_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (deployment_name, branch_name, subtype, normalized_key)
  -- Multiple normalized surface forms may intentionally resolve to one
  -- canonical, so canonical_node_id is not unique.
);

-- Different normalized identities may resolve through aliases to one
-- canonical, but two genuinely novel identities must never mint the same id.
CREATE UNIQUE INDEX IF NOT EXISTS entity_identity_claims_unique_mint_idx
  ON okg.entity_identity_claims
  (deployment_name, branch_name, subtype, canonical_node_id)
  WHERE resolution_method = 'mint';

CREATE TABLE IF NOT EXISTS okg.entity_resolution_rejections (
  rejection_id        BIGSERIAL PRIMARY KEY,
  deployment_name     TEXT NOT NULL,
  branch_name         TEXT NOT NULL,
  subtype             TEXT NOT NULL,
  normalized_key      TEXT NOT NULL,
  raw_name            TEXT NOT NULL,
  reason              TEXT NOT NULL,
  policy_fingerprint  TEXT NOT NULL,
  source_node_ref     TEXT,
  occurrences         BIGINT NOT NULL DEFAULT 1,
  first_seen_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (deployment_name, branch_name, subtype, normalized_key,
          policy_fingerprint, reason)
);

-- Durable retired-id ledger and idempotent receipt for bounded entity merges.
-- The retired snapshot remains queryable after the live node is retracted.
CREATE TABLE IF NOT EXISTS okg.entity_merge_receipts (
  merge_id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  deployment_name      TEXT NOT NULL,
  branch_name          TEXT NOT NULL,
  survivor_node_id     TEXT NOT NULL,
  retired_node_id      TEXT NOT NULL,
  subtype              TEXT NOT NULL,
  pinned_checkpoint    TEXT NOT NULL,
  catalog_version_id   BIGINT NOT NULL,
  footprint_digest     TEXT NOT NULL,
  plan_digest          TEXT NOT NULL,
  plan                  JSONB NOT NULL,
  retired_snapshot      JSONB NOT NULL,
  status                TEXT NOT NULL DEFAULT 'awaiting_publish',
  repair_generation_id  TEXT,
  actor                  TEXT NOT NULL,
  applied_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at           TIMESTAMPTZ,
  UNIQUE (deployment_name, branch_name, retired_node_id),
  UNIQUE (deployment_name, branch_name, plan_digest)
);

-- Terminal intent for a checkpoint-pinned, history-preserving source retirement.
-- Per-chunk application receipts remain in application_receipts; this row binds
-- the reviewed source/footprint plan to the generation that publishes it.
CREATE TABLE IF NOT EXISTS okg.source_retirement_receipts (
  retirement_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  deployment_name       TEXT NOT NULL,
  branch_name           TEXT NOT NULL,
  registry_source_id    TEXT NOT NULL,
  ownership_id          TEXT NOT NULL,
  source_subject_ids    JSONB NOT NULL,
  registry_digest       TEXT NOT NULL,
  pinned_checkpoint     TEXT NOT NULL,
  catalog_version_id    BIGINT NOT NULL,
  branch_mutation_token TEXT NOT NULL,
  footprint_digest      TEXT NOT NULL,
  plan_digest           TEXT NOT NULL,
  plan                  JSONB NOT NULL,
  batch_receipts        JSONB NOT NULL DEFAULT '[]'::jsonb,
  status                TEXT NOT NULL DEFAULT 'planned',
  repair_generation_id  TEXT,
  actor                  TEXT NOT NULL,
  applied_at             TIMESTAMPTZ,
  published_at           TIMESTAMPTZ,
  UNIQUE (deployment_name, branch_name, registry_source_id),
  UNIQUE (deployment_name, branch_name, plan_digest)
);

ALTER TABLE okg.source_retirement_receipts
  ALTER COLUMN batch_receipts SET DEFAULT '[]'::jsonb,
  ALTER COLUMN status SET DEFAULT 'planned',
  ALTER COLUMN applied_at DROP NOT NULL;

-- GIN trigram index for fuzzy alias resolution. The cascade in
-- okg.fuzzy_resolve_alias relies on this index to skip
-- non-candidate rows when running `WHERE alias % needle`.
-- Created idempotently; safe across re-migrates.
CREATE INDEX IF NOT EXISTS aliases_alias_trgm_idx
  ON okg.aliases USING gin (alias gin_trgm_ops);

-- Catalog of identifier-extraction regex patterns. One row per
-- (id_type, regex) pair, pinned to a catalog_version_id so the
-- pattern set rolls forward / back with the rest of the
-- ontology. validator_kind names a Python-side post-match check
-- (e.g. 'doi_checksum'); NULL means "no further validation."
CREATE TABLE IF NOT EXISTS okg.identifier_patterns (
  id_type             TEXT    NOT NULL,
  regex               TEXT    NOT NULL,
  validator_kind      TEXT,
  description         TEXT,
  catalog_version_id  BIGINT  NOT NULL
    REFERENCES okg.catalog_versions(catalog_version_id),
  PRIMARY KEY (id_type, regex, catalog_version_id),
  CHECK (
    validator_kind IS NULL
    OR validator_kind IN ('doi_checksum', 'orcid_checksum', 'isbn_checksum')
  )
);

CREATE INDEX IF NOT EXISTS identifier_patterns_cv_idx
  ON okg.identifier_patterns (catalog_version_id);


-- Set-returning function: apply every identifier pattern at the
-- given catalog version against `p_text` and return one row per
-- match. Uses Postgres-17 regexp_instr/regexp_substr to walk
-- occurrences without re-scanning the input per occurrence.
-- Spans are returned 0-indexed (Python convention) so callers
-- consuming through psycopg can use the values directly as
-- string slice indices.
--
-- STABLE: deterministic function of (text, catalog_version_id).
-- PARALLEL SAFE: only reads from immutable inputs + a STABLE
-- table read.
CREATE OR REPLACE FUNCTION okg.find_identifier_mentions(
  p_text  TEXT,
  p_cv    BIGINT
) RETURNS TABLE (
  id_type    TEXT,
  value      TEXT,
  span_start INTEGER,
  span_end   INTEGER
)
LANGUAGE plpgsql STABLE PARALLEL SAFE AS $$
DECLARE
  pat_rec  RECORD;
  occ      INTEGER;
  s_pos    INTEGER;  -- 1-indexed start (0 = no match)
  e_pos    INTEGER;  -- 1-indexed pos after match
BEGIN
  FOR pat_rec IN
    SELECT ip.id_type, ip.regex
    FROM okg.identifier_patterns ip
    WHERE ip.catalog_version_id = p_cv
  LOOP
    occ := 1;
    LOOP
      s_pos := regexp_instr(p_text, pat_rec.regex, 1, occ, 0);
      EXIT WHEN s_pos = 0;
      e_pos := regexp_instr(p_text, pat_rec.regex, 1, occ, 1);

      id_type    := pat_rec.id_type;
      value      := substring(p_text FROM s_pos FOR e_pos - s_pos);
      span_start := s_pos - 1;     -- 0-indexed for Python callers
      span_end   := e_pos - 1;     -- exclusive end, 0-indexed
      RETURN NEXT;

      occ := occ + 1;
    END LOOP;
  END LOOP;

  RETURN;
END;
$$;

-- Issue #3469: the `text` a searchable-text stamp writes for a producer
-- body over the declared parts. A part flagged droppable (a declared
-- value, never a path or CamelCase split) is left out when the body holds
-- it as a whole token: its exact text occurs in the body with no ASCII
-- letter or digit immediately before or after that occurrence. The other
-- parts are joined by a blank line and the body is appended byte for byte,
-- unless its trimmed form equals a part still present; with no part left
-- the text is the body alone. Mirrors
-- okg.substrate.searchable_text._text_with_body and _body_holds.
--
-- A part of up to 1000 characters is tested with one regex: every ASCII
-- punctuation character is escaped with a backslash (`\k` is the literal
-- `k` for a non-alphanumeric `k`; letters, digits, spaces and non-ASCII
-- characters are already literal), wrapped in ASCII-only lookarounds as in
-- the Python `(?<![A-Za-z0-9])...(?![A-Za-z0-9])`. A longer part would
-- exceed the regex compiler's size limit (near 20 000 characters), so it
-- walks its occurrences with `strpos` instead and tests each neighbour by
-- code point. Both try every occurrence start, as `re.search` does.
CREATE OR REPLACE FUNCTION okg._searchable_text_with_body(
  p_parts     TEXT[],
  p_droppable BOOLEAN[],
  p_body      TEXT
) RETURNS TEXT
LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS $$
DECLARE
  kept_parts TEXT[] := ARRAY[]::TEXT[];
  part      TEXT;
  i         INT;
  pos       INT;
  next_pos  INT;
  code      INT;
  held      BOOLEAN;
BEGIN
  FOR i IN 1 .. coalesce(cardinality(p_parts), 0) LOOP
    part := p_parts[i];
    held := false;
    IF p_droppable[i] AND strpos(p_body, part) > 0 THEN
      IF length(part) <= 1000 THEN
        held := p_body ~ (
          '(?<![A-Za-z0-9])'
          || regexp_replace(part, '([][!"#$%&''()*+,./:;<=>?@\\^_`{|}~-])', '\\\1', 'g')
          || '(?![A-Za-z0-9])'
        );
      ELSE
        pos := strpos(p_body, part);
        WHILE pos > 0 AND NOT held LOOP
          held := true;
          IF pos > 1 THEN
            code := ascii(substr(p_body, pos - 1, 1));
            IF code BETWEEN 48 AND 57 OR code BETWEEN 65 AND 90
               OR code BETWEEN 97 AND 122 THEN
              held := false;
            END IF;
          END IF;
          IF held AND pos + length(part) <= length(p_body) THEN
            code := ascii(substr(p_body, pos + length(part), 1));
            IF code BETWEEN 48 AND 57 OR code BETWEEN 65 AND 90
               OR code BETWEEN 97 AND 122 THEN
              held := false;
            END IF;
          END IF;
          IF NOT held THEN
            next_pos := strpos(substr(p_body, pos + 1), part);
            pos := CASE WHEN next_pos = 0 THEN 0 ELSE pos + next_pos END;
          END IF;
        END LOOP;
      END IF;
    END IF;
    IF NOT held THEN
      kept_parts := kept_parts || part;
    END IF;
  END LOOP;
  IF cardinality(kept_parts) = 0 THEN
    RETURN p_body;
  END IF;
  IF btrim(p_body, E' \t\n\r\f\v') = ANY(kept_parts) THEN
    RETURN array_to_string(kept_parts, E'\n\n');
  END IF;
  RETURN array_to_string(kept_parts, E'\n\n') || E'\n\n' || p_body;
END;
$$;

-- Per `add-text-attr-declaration` (2026-04-25): derive a node's
-- `attrs.text` from the subtype's declared
-- `metadata->'searchable_text_fields'` array. Returns `attrs`
-- unchanged when the subtype has no declaration; otherwise
-- writes `attrs.text` as the `\n\n`-joined non-empty string values
-- at those keys (in declared order), plus the path-field splits.
--
-- Issue #2991: a producer's own `text` body is appended as the LAST
-- part, byte for byte (or not repeated, when it equals a part), and kept
-- under the reserved key `okg_producer_text` whenever a declared part
-- exists. A restamp reads the body from that copy while `text` is exactly
-- the stamp the copy gives, never from the old rollup, so stamping the
-- result again returns it unchanged (restamping the rollup itself would
-- give `fresh\n\nold_rollup`), and any other supplied `text` wins over a stale
-- copy. With no declared part the body is the whole text, stored unchanged
-- (issue #2972) and with no copy. With no body the text is the plain
-- rollup, as before.
--
-- Issue #3469: with a body, a declared value the body already holds as a
-- whole token is left out (`okg._searchable_text_with_body`); path and
-- CamelCase splits always stay. The copy is kept even when every part was
-- left out. A node stored under the #2991 rule (every part, then the body)
-- is recognised by its copy, so it restamps to the shorter form and its
-- old text is never read as a new body. Mirrors
-- okg.substrate.searchable_text.stamp_searchable_text;
-- tests/substrate/test_searchable_text_path_fields.py pins the two.
--
-- STABLE: deterministic in attrs + catalog state.
CREATE OR REPLACE FUNCTION okg._stamp_searchable_text(
  p_subtype TEXT,
  p_attrs   JSONB,
  p_cv      BIGINT
) RETURNS JSONB
LANGUAGE plpgsql STABLE PARALLEL SAFE AS $$
DECLARE
  fields    JSONB;
  path_fields JSONB;
  parts     TEXT[] := ARRAY[]::TEXT[];
  -- Parallel to `parts`: true for a declared value (droppable when the body
  -- holds it), false for a path or CamelCase split (always kept).
  droppable BOOLEAN[] := ARRAY[]::BOOLEAN[];
  field_key TEXT;
  value     TEXT;
  split_value TEXT;
  camel_parts TEXT[];
  camel_value TEXT;
  word      TEXT;
  text_value TEXT;
  kept_value TEXT;
  kept_stamp TEXT;
  legacy_stamp TEXT;
  body      TEXT;
  rollup    TEXT;
  stamped   JSONB;
BEGIN
  IF p_cv IS NULL THEN
    RETURN p_attrs;
  END IF;
  SELECT s.metadata->'searchable_text_path_fields' INTO path_fields
    FROM okg.subtypes s
   WHERE s.name = p_subtype
     AND s.catalog_version_id = p_cv;
  SELECT s.metadata->'searchable_text_fields' INTO fields
    FROM okg.subtypes s
   WHERE s.name = p_subtype
     AND s.catalog_version_id = p_cv;
  IF fields IS NULL OR jsonb_typeof(fields) <> 'array'
     OR jsonb_array_length(fields) = 0 THEN
    RETURN p_attrs;
  END IF;
  FOR field_key IN SELECT jsonb_array_elements_text(fields) LOOP
    -- `text` is the output slot and `okg_producer_text` its reserved
    -- input: neither is a declared part. The producer body they carry is
    -- appended after every part, below.
    IF field_key IN ('text', 'okg_producer_text') THEN
      CONTINUE;
    END IF;
    -- Per `refine-doc-corpus-extraction` (2026-04-26) W8:
    -- skip array / object jsonb values so multi-valued fields
    -- (e.g. parse_warnings = ["oversize"]) don't get folded
    -- into the BM25-indexed text. Only string scalars
    -- contribute.
    IF jsonb_typeof(p_attrs->field_key) <> 'string' THEN
      CONTINUE;
    END IF;
    value := p_attrs->>field_key;
    -- Order-preserving dedupe: two declared fields can hold the same
    -- value (Document's `title` is seeded from `path`); repeating it only
    -- inflates BM25 term frequency.
    --
    -- `btrim(value, ASCII whitespace)`, not bare `trim(value)` (spaces
    -- only): the Python mirror strips the ASCII whitespace set, and with
    -- a dedupe in play a padding difference no longer just changes
    -- padding — it changes whether a part is dropped, so the two write
    -- paths would stamp structurally different text. See
    -- okg.substrate.searchable_text.ASCII_WHITESPACE.
    value := btrim(value, E' \t\n\r\f\v');
    IF value IS NOT NULL AND length(value) > 0
       AND NOT (value = ANY(parts)) THEN
      parts := parts || value;
      droppable := droppable || true;
    END IF;
  END LOOP;
  -- Per `index-document-source-file-search` (issue #900): fields named in
  -- `searchable_text_path_fields` hold structured identifiers (paths).
  -- The BM25 index is built WITH (text_config='english'), and that parser
  -- lexes a whole slash-path as a SINGLE `file` token, so an interior word
  -- can never match. Append the component-word form alongside the raw
  -- value. Skipped when already present, so subtypes that do not declare
  -- path fields stamp byte-identical text to the pre-change function.
  -- Mirrors okg.substrate.searchable_text.split_path_value.
  IF jsonb_typeof(path_fields) = 'array' THEN
    FOR field_key IN SELECT jsonb_array_elements_text(path_fields) LOOP
      IF field_key IN ('text', 'okg_producer_text') THEN
        CONTINUE;
      END IF;
      IF jsonb_typeof(p_attrs->field_key) <> 'string' THEN
        CONTINUE;
      END IF;
      value := p_attrs->>field_key;
      IF value IS NULL
         OR length(btrim(value, E' \t\n\r\f\v')) = 0 THEN
        CONTINUE;
      END IF;
      -- `[ \t\n\r\f\v]`, not `\s`: Python's `\s` also matches U+00A0 and
      -- the other Unicode spaces while Postgres' does not, so `\s` here
      -- would make the mirrors disagree on any value with a non-breaking
      -- space. Same set on both sides.
      split_value := btrim(
        regexp_replace(
          regexp_replace(value, '[/\\_.:-]+', ' ', 'g'),
          '[ \t\n\r\f\v]+', ' ', 'g'
        ),
        E' \t\n\r\f\v'
      );
      IF length(split_value) > 0 AND NOT (split_value = ANY(parts)) THEN
        parts := parts || split_value;
        droppable := droppable || false;
      END IF;
      -- archi-physics/cms-kb#6: a TWiki title such as `SWGuideCrab` is one
      -- lexeme, so "crab guide" never matched. Append a SECOND form with
      -- each component split at CamelCase and letter-digit boundaries --
      -- beside the separator split, never instead of it, so a whole
      -- component (`AdmissionSmoke` in a path) stays matchable. Mirrors
      -- okg.substrate.searchable_text.split_camel_value: the same four
      -- capture-group substitutions in the same order (lower->Upper, end
      -- of an acronym, digit->Capitalized word, 2+ lowercase->digit), and
      -- hash-like components (hex only, 7+ long) left whole.
      camel_parts := ARRAY[]::TEXT[];
      FOREACH word IN ARRAY string_to_array(split_value, ' ') LOOP
        IF word !~ '^[0-9A-Fa-f]{7,}$' THEN
          word := regexp_replace(
            regexp_replace(
              regexp_replace(
                regexp_replace(word, '([a-z])([A-Z])', '\1 \2', 'g'),
                '([A-Z])([A-Z][a-z])', '\1 \2', 'g'
              ),
              '([0-9])([A-Z][a-z])', '\1 \2', 'g'
            ),
            '([a-z]{2})([0-9])', '\1 \2', 'g'
          );
        END IF;
        camel_parts := camel_parts || word;
      END LOOP;
      camel_value := array_to_string(camel_parts, ' ');
      IF length(camel_value) > 0 AND NOT (camel_value = ANY(parts)) THEN
        parts := parts || camel_value;
        droppable := droppable || false;
      END IF;
    END LOOP;
  END IF;
  IF cardinality(parts) > 0 THEN
    rollup := array_to_string(parts, E'\n\n');
  END IF;
  -- The producer body (issue #2991). The reserved copy wins only while
  -- `text` is exactly the stamp that copy gives over the current parts (a
  -- restamp of a stored node), under this rule or the #2991 rule before
  -- #3469 (every part, then the body). Otherwise a non-blank `text` is the
  -- body unless it is exactly the rollup, which is what a stamp with no body
  -- wrote. The ASCII-set `btrim` only decides whether a value counts as
  -- empty or repeats a part; the body itself is kept byte for byte.
  IF jsonb_typeof(p_attrs->'text') = 'string' THEN
    text_value := p_attrs->>'text';
  END IF;
  IF jsonb_typeof(p_attrs->'okg_producer_text') = 'string' THEN
    kept_value := p_attrs->>'okg_producer_text';
  END IF;
  -- The stamps the copy gives, computed first: a PL/pgSQL IF condition ends
  -- at its first THEN, so a CASE cannot sit inside it.
  IF kept_value IS NOT NULL
     AND length(btrim(kept_value, E' \t\n\r\f\v')) > 0 THEN
    kept_stamp := okg._searchable_text_with_body(parts, droppable, kept_value);
    IF rollup IS NULL THEN
      legacy_stamp := kept_value;
    ELSIF btrim(kept_value, E' \t\n\r\f\v') = ANY(parts) THEN
      legacy_stamp := rollup;
    ELSE
      legacy_stamp := rollup || E'\n\n' || kept_value;
    END IF;
  END IF;
  IF kept_stamp IS NOT NULL
     AND text_value IS NOT NULL
     AND (text_value = kept_stamp OR text_value = legacy_stamp) THEN
    body := kept_value;
  ELSIF text_value IS NOT NULL
        AND length(btrim(text_value, E' \t\n\r\f\v')) > 0
        AND text_value IS DISTINCT FROM rollup THEN
    body := text_value;
  END IF;
  stamped := p_attrs - 'okg_producer_text';
  IF body IS NOT NULL THEN
    IF rollup IS NULL THEN
      RETURN jsonb_set(stamped, '{text}', to_jsonb(body), true);
    END IF;
    stamped := jsonb_set(
      stamped, '{text}',
      to_jsonb(okg._searchable_text_with_body(parts, droppable, body)), true
    );
    RETURN jsonb_set(stamped, '{okg_producer_text}', to_jsonb(body), true);
  END IF;
  IF rollup IS NOT NULL THEN
    RETURN jsonb_set(stamped, '{text}', to_jsonb(rollup), true);
  END IF;
  RETURN stamped;
END;
$$;

-- Per `auto-promote-semantic-timestamps` (2026-04-26):
-- copy a subtype's declared semantic timestamp source field
-- into `attrs.created_at` / `attrs.last_modified_at` only
-- when the target is absent. Hand-stamped targets win.
-- Idempotent. STABLE: deterministic in attrs + catalog.
CREATE OR REPLACE FUNCTION okg._stamp_semantic_timestamps(
  p_subtype TEXT,
  p_attrs   JSONB,
  p_cv      BIGINT
) RETURNS JSONB
LANGUAGE plpgsql STABLE PARALLEL SAFE AS $$
DECLARE
  meta            JSONB;
  src_field       TEXT;
  src_value       TEXT;
  out_attrs       JSONB := p_attrs;
BEGIN
  IF p_cv IS NULL OR p_attrs IS NULL THEN
    RETURN p_attrs;
  END IF;
  SELECT s.metadata INTO meta
    FROM okg.subtypes s
   WHERE s.name = p_subtype
     AND s.catalog_version_id = p_cv;
  IF meta IS NULL THEN
    RETURN p_attrs;
  END IF;

  -- created_at promotion.
  IF jsonb_typeof(meta->'created_at_from') = 'string'
     AND jsonb_typeof(out_attrs->'created_at') IS DISTINCT FROM 'string'
  THEN
    src_field := meta->>'created_at_from';
    IF jsonb_typeof(out_attrs->src_field) = 'string' THEN
      src_value := out_attrs->>src_field;
      IF src_value IS NOT NULL AND length(src_value) > 0 THEN
        out_attrs := jsonb_set(
          out_attrs, '{created_at}', to_jsonb(src_value), true
        );
      END IF;
    END IF;
  END IF;

  -- last_modified_at promotion.
  IF jsonb_typeof(meta->'last_modified_at_from') = 'string'
     AND jsonb_typeof(out_attrs->'last_modified_at') IS DISTINCT FROM 'string'
  THEN
    src_field := meta->>'last_modified_at_from';
    IF jsonb_typeof(out_attrs->src_field) = 'string' THEN
      src_value := out_attrs->>src_field;
      IF src_value IS NOT NULL AND length(src_value) > 0 THEN
        out_attrs := jsonb_set(
          out_attrs, '{last_modified_at}', to_jsonb(src_value), true
        );
      END IF;
    END IF;
  END IF;

  RETURN out_attrs;
END;
$$;

-- Cascade alias resolver: exact -> trigram -> Levenshtein.
-- Returns highest-scoring matches above threshold, capped at
-- p_limit. The cascade short-circuits at the first tier that
-- returns rows above the threshold (RETURN inside plpgsql is
-- a no-op when the prior RETURN QUERY filled rows; we use
-- IF FOUND to enforce true short-circuit).
CREATE OR REPLACE FUNCTION okg.fuzzy_resolve_alias(
  p_needle    TEXT,
  p_cv        BIGINT,
  p_threshold REAL    DEFAULT 0.6,
  p_limit     INTEGER DEFAULT 5
) RETURNS TABLE (
  canonical   TEXT,
  similarity  REAL,
  method      TEXT
)
LANGUAGE plpgsql STABLE AS $$
DECLARE
  lev_cap INTEGER := GREATEST(2, length(p_needle) / 4);
BEGIN
  -- Tier 1: exact match
  RETURN QUERY
    SELECT
      a.canonical,
      1.0::real             AS similarity,
      'exact'::text         AS method
    FROM okg.aliases a
    WHERE a.alias = p_needle
      AND a.catalog_version_id = p_cv
    LIMIT p_limit;
  IF FOUND THEN RETURN; END IF;

  -- Tier 2: trigram similarity (uses GIN index)
  RETURN QUERY
    SELECT
      a.canonical,
      similarity(a.alias, p_needle)::real AS similarity,
      'trigram'::text                      AS method
    FROM okg.aliases a
    WHERE a.catalog_version_id = p_cv
      AND a.alias % p_needle
      AND similarity(a.alias, p_needle) >= p_threshold
    ORDER BY similarity(a.alias, p_needle) DESC
    LIMIT p_limit;
  IF FOUND THEN RETURN; END IF;

  -- Tier 3: Levenshtein (capped to avoid O(N*L) on long needles)
  -- fuzzystrmatch's Levenshtein functions reject inputs over
  -- 255 characters. Exact/trigram tiers above can still resolve
  -- long strings; if they miss, skip edit-distance instead of
  -- raising and poisoning the caller's whole batch.
  IF length(p_needle) > 255 THEN RETURN; END IF;

  RETURN QUERY
    SELECT
      a.canonical,
      (
        1.0
        - (
            levenshtein_less_equal(a.alias, p_needle, lev_cap)::real
            / GREATEST(length(a.alias), length(p_needle))::real
          )
      )::real                              AS similarity,
      'levenshtein'::text                  AS method
    FROM okg.aliases a
    WHERE a.catalog_version_id = p_cv
      AND length(a.alias) <= 255
      AND levenshtein_less_equal(a.alias, p_needle, lev_cap) <= lev_cap
    ORDER BY similarity DESC
    LIMIT p_limit;
END;
$$;

CREATE TABLE IF NOT EXISTS okg.invariants (
  name                TEXT    NOT NULL,
  severity            TEXT    NOT NULL CHECK (severity IN ('error','warn')),
  scope               TEXT    NOT NULL CHECK (scope IN ('source','cross')),
  source              TEXT,    -- required when scope='source'
  sql_body            TEXT,
  -- Per-invariant statement_timeout (ms) applied inside the Phase B
  -- evaluator (M5.2). NULL = use deployment default (5000 ms).
  timeout_ms          INTEGER,
  -- `kind` discriminates SQL invariants (the existing path; row source
  -- is `sql_body` evaluated inside the Phase B gate) from LLM-judge
  -- invariants (per `add-semantic-llm-judge-invariants`; row source is
  -- `candidates_sql`, evaluated by `judge_model` outside the gate via
  -- `okg run --include-llm-invariants`). Defaults to 'sql' for back
  -- compat — invariants installed pre-LLM-judge omit the field.
  kind                TEXT    NOT NULL DEFAULT 'sql'
                              CHECK (kind IN ('sql','llm_judge')),
  -- LLM-judge fields. Required iff kind='llm_judge', forbidden
  -- otherwise. The CHECK constraint enforces the bi-conditional so a
  -- malformed install fails at the DB even if the loader regresses.
  candidates_sql      TEXT,
  judge_prompt        TEXT,    -- path to the prompt file (relative to YAML)
  judge_model         TEXT,
  sample_cap          INTEGER,
  fire_on             TEXT[],  -- which verdicts count as fired (e.g. {'inconsistent'})
  -- NOTE: judge_spend_cap_usd / judge_spend_estimate_usd are added by
  -- the guarded ALTER below (after concept_scope), NOT inline here:
  -- deployments that predate them get the columns appended by ALTER, so
  -- the snapshot must append them in the same order or fresh-install
  -- and migrated-install column order diverge (the migration-convergence
  -- lane diffs the two shapes and fails on any difference).
  catalog_version_id  BIGINT  NOT NULL
    REFERENCES okg.catalog_versions(catalog_version_id),
  PRIMARY KEY (name, catalog_version_id),
  CHECK (scope <> 'source' OR source IS NOT NULL),
  CHECK (timeout_ms IS NULL OR timeout_ms > 0),
  -- SQL kind: sql_body required, candidates_sql / judge_* / sample_cap
  -- forbidden. LLM-judge kind: candidates_sql / judge_prompt /
  -- judge_model / sample_cap required, sql_body forbidden.
  CHECK (
    (kind = 'sql'       AND sql_body IS NOT NULL
                        AND candidates_sql IS NULL
                        AND judge_prompt   IS NULL
                        AND judge_model    IS NULL
                        AND sample_cap     IS NULL)
    OR
    (kind = 'llm_judge' AND sql_body       IS NULL
                        AND candidates_sql IS NOT NULL
                        AND judge_prompt   IS NOT NULL
                        AND judge_model    IS NOT NULL
                        AND sample_cap     IS NOT NULL
                        AND sample_cap > 0)
  )
);

-- M5 added timeout_ms column. Idempotent upgrade for pre-M5
-- schemas; ADD IF NOT EXISTS is PG 9.6+.
ALTER TABLE okg.invariants
  ADD COLUMN IF NOT EXISTS timeout_ms INTEGER;

-- `add-semantic-llm-judge-invariants` (2026-04-29) added LLM-judge
-- columns. Idempotent upgrade for pre-LLM-judge schemas. The
-- bi-conditional CHECK above is added separately via DROP+ADD so it
-- doesn't conflict with an existing CHECK on legacy databases.
ALTER TABLE okg.invariants
  ADD COLUMN IF NOT EXISTS kind            TEXT NOT NULL DEFAULT 'sql',
  ADD COLUMN IF NOT EXISTS candidates_sql  TEXT,
  ADD COLUMN IF NOT EXISTS judge_prompt    TEXT,
  ADD COLUMN IF NOT EXISTS judge_model     TEXT,
  ADD COLUMN IF NOT EXISTS sample_cap      INTEGER,
  ADD COLUMN IF NOT EXISTS fire_on         TEXT[];

ALTER TABLE okg.invariants
  ALTER COLUMN sql_body DROP NOT NULL;

-- `add-concept-scoped-invariants` (capability E of the af_pubs
-- supervisor demo) adds an optional concept_scope JSONB carrying
-- `{concept_name, min_confidence, hops?, hop_edges?}`. NULL ⇒
-- unscoped (deployment-wide). Set ⇒ the runner injects a `scope`
-- CTE before the user's candidates_sql / sql_body that filters
-- nodes to the named Concept's neighborhood.
ALTER TABLE okg.invariants
  ADD COLUMN IF NOT EXISTS concept_scope JSONB;

-- `coherent-parallelism` / model-call-admission: judge-lane spend
-- admission. The cap bounds this invariant's total judge spend per run;
-- the estimate is reserved per call before dispatch. Both-or-neither
-- (loader-enforced); both NULL = spend-unbounded, which the runner
-- reports. Added HERE (after concept_scope, never inline in CREATE
-- TABLE) so fresh installs append the columns in the same order
-- existing deployments received them — the migration-convergence lane
-- diffs fresh vs migrated shapes and fails on any difference. The CHECK
-- constraints are added guarded (ADD CONSTRAINT has no IF NOT EXISTS)
-- so migrated databases converge to the same constrained shape.
ALTER TABLE okg.invariants
  ADD COLUMN IF NOT EXISTS judge_spend_cap_usd      NUMERIC,
  ADD COLUMN IF NOT EXISTS judge_spend_estimate_usd NUMERIC;

DO $okg_invariants_judge_spend_checks$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conname = 'invariants_judge_spend_cap_usd_check'
       AND conrelid = 'okg.invariants'::regclass
  ) THEN
    ALTER TABLE okg.invariants
      ADD CONSTRAINT invariants_judge_spend_cap_usd_check
      CHECK (judge_spend_cap_usd > 0);
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conname = 'invariants_judge_spend_estimate_usd_check'
       AND conrelid = 'okg.invariants'::regclass
  ) THEN
    ALTER TABLE okg.invariants
      ADD CONSTRAINT invariants_judge_spend_estimate_usd_check
      CHECK (judge_spend_estimate_usd > 0);
  END IF;
END
$okg_invariants_judge_spend_checks$;

-- Issue #3624: sources an invariant's data comes from. NULL = always
-- evaluated. Set = the publish gate skips the invariant, and reports it as
-- skipped, while any named source is off in the deployment's registry.
-- Appended after the judge-spend columns for the same fresh-vs-migrated
-- column-order reason.
ALTER TABLE okg.invariants
  ADD COLUMN IF NOT EXISTS requires_sources TEXT[];

-- ========== Fact log (Timescale hypertables) ==========

CREATE TABLE IF NOT EXISTS okg.node_facts (
  fact_id             BIGSERIAL,
  node_id             TEXT       NOT NULL,
  subtype             TEXT       NOT NULL,
  attrs               JSONB      NOT NULL DEFAULT '{}'::jsonb,
  source              TEXT       NOT NULL,
  source_record_id    JSONB      NOT NULL,
  source_revision     JSONB      NOT NULL,
  observed_at         TIMESTAMPTZ NOT NULL,
  -- `dedupe_key` is a hash over (source, source_record_id,
  -- source_revision, op). The UNIQUE on (dedupe_key, observed_at)
  -- enforces "the same fact arriving twice in the same instant
  -- collapses." Cross-time dedup (the same source emitting the same
  -- (record_id, revision) tomorrow) is handled by the projection's
  -- revision-kind ordering, not by this UNIQUE. See the M1
  -- design-review note in phase0-surprises.md (M1.5 batch).
  dedupe_key          TEXT       NOT NULL,
  run_id              UUID       NOT NULL,
  op                  CHAR(1)    NOT NULL CHECK (op IN ('I','R')),
  -- Primary key on a hypertable MUST include the partition column.
  PRIMARY KEY (fact_id, observed_at),
  UNIQUE (dedupe_key, observed_at)
);

CREATE TABLE IF NOT EXISTS okg.edge_facts (
  fact_id             BIGSERIAL,
  edge_id             BIGINT     NOT NULL,
  src                 TEXT       NOT NULL,
  dst                 TEXT       NOT NULL,
  edge_type           TEXT       NOT NULL,
  attrs               JSONB      NOT NULL DEFAULT '{}'::jsonb,
  provenance          TEXT       NOT NULL
    CHECK (provenance IN ('explicit','derived_deterministic',
                           'linked_heuristic','extracted_model',
                           'derived_semantic')),
  confidence          REAL       NOT NULL DEFAULT 1.0
    CHECK (confidence >= 0 AND confidence <= 1),
  source              TEXT       NOT NULL,
  source_record_id    JSONB      NOT NULL,
  source_revision     JSONB      NOT NULL,
  observed_at         TIMESTAMPTZ NOT NULL,
  -- See node_facts.dedupe_key comment.
  dedupe_key          TEXT       NOT NULL,
  run_id              UUID       NOT NULL,
  op                  CHAR(1)    NOT NULL CHECK (op IN ('I','R')),
  PRIMARY KEY (fact_id, observed_at),
  UNIQUE (dedupe_key, observed_at)
);

-- Sequence for edge_id (shared across fact rows for the same logical
-- edge). Referenced by fact writers so the role tests and the
-- bootstrap harness can insert.
CREATE SEQUENCE IF NOT EXISTS okg.edge_facts_edge_id_seq;

-- Promote to Timescale hypertables. create_hypertable is idempotent
-- when if_not_exists => TRUE.
SELECT create_hypertable(
  'okg.node_facts', 'observed_at',
  chunk_time_interval => INTERVAL '7 days',
  if_not_exists => TRUE,
  migrate_data => TRUE
);
SELECT create_hypertable(
  'okg.edge_facts', 'observed_at',
  chunk_time_interval => INTERVAL '7 days',
  if_not_exists => TRUE,
  migrate_data => TRUE
);

-- Indexes on fact tables. Hypertable indexes are chunk-local.
CREATE INDEX IF NOT EXISTS node_facts_node_id_observed_idx
  ON okg.node_facts (node_id, observed_at DESC);
CREATE INDEX IF NOT EXISTS node_facts_source_record_idx
  ON okg.node_facts (source, (source_record_id::text), (source_revision::text));
CREATE INDEX IF NOT EXISTS node_facts_source_run_observed_idx
  ON okg.node_facts (source, run_id, observed_at DESC);
-- The projection cursor and every per-run replay walk by run_id;
-- without this index they scan every chunk. Per the M1 design review.
CREATE INDEX IF NOT EXISTS node_facts_run_id_idx
  ON okg.node_facts (run_id, fact_id);
-- Gate-cache keys need the latest workload-source fact id, excluding
-- materializer/derived-artifact output facts. Keep that max lookup bounded even
-- when the fact-log tail is dominated by derived outputs.
CREATE INDEX IF NOT EXISTS node_facts_workload_fact_idx
  ON okg.node_facts (fact_id DESC)
  WHERE source NOT LIKE 'materializer:%'
    AND source NOT LIKE 'derived_artifact:%';
-- Emit-once probe (PACT add-emit-once-dedupe-scope, issue #901). The
-- enricher emit helpers support `dedupe_scope="key"`, which suppresses a
-- fact whose (source, dedupe_key, op) is already present at ANY
-- observed_at. That probe cannot be an ON CONFLICT against a unique
-- index: these are hypertables partitioned on observed_at, and Timescale
-- rejects a unique index that omits the partitioning column. So it is a
-- WHERE NOT EXISTS, and without an index it scans every chunk -- up to
-- 180 days of retention at 7 days per chunk. Column order matches the
-- probe's equality predicate exactly.
--
-- PARTIAL, deliberately. These are the two hottest tables in the system:
-- every source's admission traffic lands here, and an unconditional
-- btree over (TEXT, TEXT, CHAR) would tax all of it to serve a probe
-- that only the enricher emit helpers ever issue. Those helpers always
-- write `source = '_enricher:<name>'`, so this predicate covers 100% of
-- probes while indexing a small fraction of rows. The probe spells the
-- same LIKE out as a constant so the planner can prove the implication;
-- test_probe_can_use_the_partial_index fails if that stops holding.
CREATE INDEX IF NOT EXISTS node_facts_dedupe_probe_idx
  ON okg.node_facts (source, dedupe_key, op)
  WHERE source LIKE '\_enricher:%';
CREATE INDEX IF NOT EXISTS edge_facts_src_edge_dst_observed_idx
  ON okg.edge_facts (src, edge_type, dst, observed_at DESC);
CREATE INDEX IF NOT EXISTS edge_facts_source_op_src_edge_dst_observed_idx
  ON okg.edge_facts (source, op, src, edge_type, dst, observed_at DESC);
-- Edge half of the emit-once probe index; see the node_facts comment
-- above for why it is partial.
CREATE INDEX IF NOT EXISTS edge_facts_dedupe_probe_idx
  ON okg.edge_facts (source, dedupe_key, op)
  WHERE source LIKE '\_enricher:%';
CREATE INDEX IF NOT EXISTS edge_facts_source_record_idx
  ON okg.edge_facts (source, (source_record_id::text), (source_revision::text));
CREATE INDEX IF NOT EXISTS edge_facts_source_run_observed_idx
  ON okg.edge_facts (source, run_id, observed_at DESC);
CREATE INDEX IF NOT EXISTS edge_facts_run_id_idx
  ON okg.edge_facts (run_id, fact_id);
CREATE INDEX IF NOT EXISTS edge_facts_workload_fact_idx
  ON okg.edge_facts (fact_id DESC)
  WHERE source NOT LIKE 'materializer:%'
    AND source NOT LIKE 'derived_artifact:%';
-- Hot path for Publisher.project_new_edges: every publish projects
-- op='I' edge facts past the previous edge_fact_watermark. Keep
-- fact_id first so incremental publishes scan only new edge facts;
-- INCLUDE carries the projected columns for an index-only scan.
CREATE INDEX IF NOT EXISTS edge_facts_project_i_fact_idx
  ON okg.edge_facts (fact_id, edge_id)
  INCLUDE (src, dst, edge_type, attrs)
  WHERE op = 'I';
-- Optional dst-side traversal index (M1 design-review flag). Cheap to
-- carry; if M3's projection workload doesn't touch it we revisit.
CREATE INDEX IF NOT EXISTS edge_facts_dst_edge_observed_idx
  ON okg.edge_facts (dst, edge_type, observed_at DESC);
-- ========== Admission envelope ==========
--
-- Admission records are the durable "why/how was this mutation batch
-- allowed to enter the substrate?" ledger. Source facts remain the
-- high-volume append log; admission rows are compact, idempotent, and
-- tied to DBOS workflow attempts or operator decisions.

CREATE TABLE IF NOT EXISTS okg.producer_profiles (
  profile_id          BIGSERIAL PRIMARY KEY,
  deployment_name     TEXT NOT NULL,
  producer_id         TEXT NOT NULL,
  producer_kind       TEXT NOT NULL
    CHECK (producer_kind IN (
      'source', 'extractor', 'enricher', 'agent',
      'human', 'workflow', 'system', 'lens'
    )),
  trust_label         TEXT NOT NULL,
  admission_mode      TEXT NOT NULL
    CHECK (admission_mode IN (
      'direct', 'fast_track', 'review', 'quarantine'
    )),
  authority_scope     JSONB NOT NULL DEFAULT '{}'::jsonb,
  allowed_record_kinds TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
  output_signature    JSONB NOT NULL DEFAULT '{}'::jsonb,
  policy_clauses      JSONB NOT NULL DEFAULT '[]'::jsonb,
  output_scope_summary JSONB NOT NULL DEFAULT '{}'::jsonb,
  source_ref          JSONB NOT NULL DEFAULT '{}'::jsonb,
  catalog_version_id  BIGINT
    REFERENCES okg.catalog_versions(catalog_version_id),
  profile_fingerprint TEXT NOT NULL,
  status              TEXT NOT NULL
    CHECK (status IN ('active', 'compat', 'disabled')),
  catalog_evidence    JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (deployment_name, producer_id, profile_fingerprint)
);

CREATE INDEX IF NOT EXISTS producer_profiles_deployment_status_idx
  ON okg.producer_profiles (deployment_name, producer_id, status);
CREATE INDEX IF NOT EXISTS producer_profiles_catalog_idx
  ON okg.producer_profiles (catalog_version_id)
  WHERE catalog_version_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS okg.admission_envelopes (
  admission_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  deployment_name     TEXT NOT NULL,
  producer_id         TEXT NOT NULL,
  producer_kind       TEXT NOT NULL
    CHECK (producer_kind IN (
      'source', 'extractor', 'enricher', 'agent',
      'human', 'workflow', 'system'
    )),
  intent              TEXT NOT NULL,
  admission_mode      TEXT NOT NULL
    CHECK (admission_mode IN (
      'direct', 'fast_track', 'review', 'quarantine'
    )),
  status              TEXT NOT NULL
    CHECK (status IN (
      'pending', 'accepted', 'rejected', 'published',
      'blocked', 'failed', 'quarantined'
    )),
  catalog_version_id  BIGINT
    REFERENCES okg.catalog_versions(catalog_version_id),
  source              TEXT,
  run_id              UUID,
  -- D2 (database-schema.md §3.2): the TEXT checkpoint ref is the ONLY
  -- generation reference. The former BIGINT `target_generation_id` compat
  -- token is not authored in v1.0, and migration 0023 drops it from upgraded
  -- databases. `target_generation_ref` itself is added by the ALTER below
  -- rather than declared here: an upgraded database has it in trailing
  -- position (it arrived as a later ADD COLUMN), and the migration-convergence
  -- lane compares column ORDER, so declaring it inline would make fresh and
  -- migrated diverge on ordinal position alone.
  idempotency_key     TEXT NOT NULL,
  authority           JSONB NOT NULL DEFAULT '{}'::jsonb,
  check_summary       JSONB NOT NULL DEFAULT '{}'::jsonb,
  verdict             JSONB NOT NULL DEFAULT '{}'::jsonb,
  workflow_ref        JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  decided_at          TIMESTAMPTZ,
  published_at        TIMESTAMPTZ,
  UNIQUE (deployment_name, producer_id, idempotency_key)
);

-- Merge note (D2 vs the acknowledge-runtime-dlq compat repair): `dev` carried
-- an `ADD COLUMN IF NOT EXISTS target_generation_id BIGINT` here, because the
-- index that column fed (`admission_envelopes_generation_idx`) was still in the
-- snapshot and could outrun a schema that had lost the column. D2 retires BOTH:
-- the column is dropped by migration 0023 and the index is replaced by
-- `admission_envelopes_generation_ref_idx` on the TEXT ref below. Re-adding the
-- ALTER would resurrect the column on every schema replay AFTER 0023 has
-- already dropped it, so fresh and migrated databases would disagree forever.
-- `test_schema_replay_does_not_resurrect_the_legacy_admission_generation_id`
-- in tests/substrate/test_migrate.py is the guard.

CREATE INDEX IF NOT EXISTS admission_envelopes_deployment_status_idx
  ON okg.admission_envelopes (deployment_name, status, created_at DESC);
CREATE INDEX IF NOT EXISTS admission_envelopes_producer_idx
  ON okg.admission_envelopes (deployment_name, producer_kind, producer_id);
CREATE INDEX IF NOT EXISTS admission_envelopes_source_run_idx
  ON okg.admission_envelopes (source, run_id)
  WHERE run_id IS NOT NULL;
-- AKMON Phase B: admission envelopes that route to a non-default
-- branch (typically quarantine) carry a `target_branch_name`. NULL
-- means the envelope publishes to the deployment's default branch.
-- The publisher cycle reads this and routes the resulting
-- generation accordingly. The producer-policy admission verdict is
-- unchanged — Phase B does not let a quarantined envelope claim
-- `matched` policy on the default branch, only on its branch.
ALTER TABLE okg.admission_envelopes
  ADD COLUMN IF NOT EXISTS target_branch_name TEXT;

-- The sole generation reference (D2). COLLATE "C" so btree comparisons are
-- memcmp with abbreviated keys, matching okg.llm_calls / okg.work_ledger.
-- On an upgraded database the column already exists as plain TEXT and this is
-- a no-op; migration 0023 applies the collation there.
ALTER TABLE okg.admission_envelopes
  ADD COLUMN IF NOT EXISTS target_generation_ref TEXT COLLATE "C";

CREATE INDEX IF NOT EXISTS admission_envelopes_target_branch_idx
  ON okg.admission_envelopes (deployment_name, target_branch_name)
  WHERE target_branch_name IS NOT NULL;
CREATE INDEX IF NOT EXISTS admission_envelopes_pending_lens_idx
  ON okg.admission_envelopes (
    deployment_name,
    target_branch_name,
    created_at
  )
  WHERE producer_kind = 'lens'
    AND status IN ('pending', 'accepted');
CREATE INDEX IF NOT EXISTS admission_envelopes_generation_ref_idx
  ON okg.admission_envelopes (target_generation_ref)
  WHERE target_generation_ref IS NOT NULL;

COMMENT ON COLUMN okg.admission_envelopes.target_generation_ref IS
  'Canonical text generation reference for the Chronos generation targeted by this admission envelope.';

-- This constraint is DELIBERATELY installed NOT VALID, and is the one
-- registered exemption from the born-VALID contract
-- (src/okg/substrate/db/not_valid_exemptions.yaml, owner
-- schema-dead-weight-retirement). NOT VALID is not an oversight here, it is
-- the behavior: a database that already carries an admission envelope with a
-- blank target_generation_ref must KEEP that row readable -- it is the sole
-- record of an admission -- while every NEW blank insert is refused. That is
-- exactly what NOT VALID means, and it is asserted by
-- tests/substrate/db/test_generation_reference_contract.py, which pins both
-- halves (the legacy row still reads as '', a fresh blank insert raises).
-- VALIDATE-ing it would fail on precisely the databases it was written for;
-- dropping it would stop refusing new blanks, which is strictly worse than
-- the status quo. So it stays, registered and argued, rather than silently
-- unresolved.
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
      FROM pg_constraint
     WHERE conrelid = 'okg.admission_envelopes'::regclass
       AND conname = 'admission_envelopes_target_generation_ref_nonempty_chk'
  ) THEN
    ALTER TABLE okg.admission_envelopes
      ADD CONSTRAINT admission_envelopes_target_generation_ref_nonempty_chk
      CHECK (
        target_generation_ref IS NULL
        OR length(btrim(target_generation_ref)) > 0
      ) NOT VALID;
  END IF;
END $$;

CREATE TABLE IF NOT EXISTS okg.coordination_native_state (
  deployment_name TEXT NOT NULL,
  change_id TEXT,
  task_id TEXT,
  work_item_id TEXT NOT NULL,
  revision BIGINT NOT NULL DEFAULT 0,
  current_authority TEXT NOT NULL DEFAULT 'native',
  active_claim_owner TEXT,
  workspace_id TEXT,
  claim_state TEXT NOT NULL DEFAULT 'unclaimed',
  lease_state TEXT NOT NULL DEFAULT 'none',
  lease_fresh BOOLEAN NOT NULL DEFAULT false,
  blocked BOOLEAN NOT NULL DEFAULT false,
  blocker_reason TEXT,
  proof_attached BOOLEAN NOT NULL DEFAULT false,
  proof_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
  review_state TEXT NOT NULL DEFAULT 'none',
  merge_ready BOOLEAN NOT NULL DEFAULT false,
  supervisor_finalized BOOLEAN NOT NULL DEFAULT false,
  subagents_rolled_up BOOLEAN NOT NULL DEFAULT false,
  finalized_state TEXT,
  task_done BOOLEAN NOT NULL DEFAULT false,
  last_heartbeat_at TIMESTAMPTZ,
  last_operator_report_kind TEXT,
  last_event_id TEXT,
  attrs JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  -- Keep fresh-install column order aligned with the append-only upgrade
  -- path below.  PostgreSQL preserves physical column order in schema dumps.
  assignment_id TEXT NOT NULL,
  assignment_kind TEXT NOT NULL DEFAULT 'legacy_task',
  assignment_generation BIGINT NOT NULL DEFAULT 1,
  assignment_status TEXT NOT NULL DEFAULT 'active',
  project_id TEXT,
  sprint_id TEXT,
  parent_assignment_id TEXT,
  owner_principal TEXT,
  owner_session_id TEXT,
  git_worktree_id TEXT,
  okg_branch_id TEXT,
  -- Server-owned restricted OpenSSH enrollment and exact lease scope. These
  -- columns remain nullable for legacy task rows migrated in place.
  authenticated_host TEXT,
  credential_id TEXT,
  source_address TEXT,
  source_port INTEGER,
  target_address TEXT,
  target_port INTEGER,
  transport_verified_by TEXT,
  allowed_event_types JSONB NOT NULL DEFAULT '[]'::jsonb,
  lease_expires_at TIMESTAMPTZ,
  policy_digest TEXT,
  project_digest TEXT,
  execution_profile TEXT,
  execution_lane TEXT,
  named_resource_demand JSONB NOT NULL DEFAULT '{}'::jsonb,
  capacity_digest TEXT,
  PRIMARY KEY (deployment_name, assignment_id),
  CHECK (length(btrim(assignment_id)) > 0),
  CHECK (assignment_kind IN ('chief', 'sprint', 'pact', 'delegation', 'legacy_task')),
  CHECK (assignment_generation > 0),
  CHECK (assignment_status IN ('pending', 'starting', 'active', 'retired', 'frozen')),
  CHECK (execution_lane IS NULL OR execution_lane IN ('critical', 'light')),
  CHECK (jsonb_typeof(named_resource_demand) = 'object'),
  CHECK (parent_assignment_id IS NULL OR parent_assignment_id <> assignment_id),
  CONSTRAINT coordination_native_state_assignment_owner_check CHECK (
    assignment_kind = 'legacy_task'
    OR (
      owner_principal IS NOT NULL AND length(btrim(owner_principal)) > 0
      AND owner_session_id IS NOT NULL AND length(btrim(owner_session_id)) > 0
    )
  )
);

ALTER TABLE okg.coordination_native_state
  ADD COLUMN IF NOT EXISTS task_done BOOLEAN NOT NULL DEFAULT false;

ALTER TABLE okg.coordination_native_state
  ADD COLUMN IF NOT EXISTS assignment_id TEXT,
  ADD COLUMN IF NOT EXISTS assignment_kind TEXT,
  ADD COLUMN IF NOT EXISTS assignment_generation BIGINT,
  ADD COLUMN IF NOT EXISTS assignment_status TEXT,
  ADD COLUMN IF NOT EXISTS project_id TEXT,
  ADD COLUMN IF NOT EXISTS sprint_id TEXT,
  ADD COLUMN IF NOT EXISTS parent_assignment_id TEXT,
  ADD COLUMN IF NOT EXISTS owner_principal TEXT,
  ADD COLUMN IF NOT EXISTS owner_session_id TEXT,
  ADD COLUMN IF NOT EXISTS git_worktree_id TEXT,
  ADD COLUMN IF NOT EXISTS okg_branch_id TEXT,
  ADD COLUMN IF NOT EXISTS authenticated_host TEXT,
  ADD COLUMN IF NOT EXISTS credential_id TEXT,
  ADD COLUMN IF NOT EXISTS source_address TEXT,
  ADD COLUMN IF NOT EXISTS source_port INTEGER,
  ADD COLUMN IF NOT EXISTS target_address TEXT,
  ADD COLUMN IF NOT EXISTS target_port INTEGER,
  ADD COLUMN IF NOT EXISTS transport_verified_by TEXT,
  ADD COLUMN IF NOT EXISTS allowed_event_types JSONB NOT NULL DEFAULT '[]'::jsonb,
  ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS policy_digest TEXT,
  ADD COLUMN IF NOT EXISTS project_digest TEXT,
  ADD COLUMN IF NOT EXISTS execution_profile TEXT,
  ADD COLUMN IF NOT EXISTS execution_lane TEXT,
  ADD COLUMN IF NOT EXISTS named_resource_demand JSONB NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS capacity_digest TEXT;

-- Preserve the old task-shaped key as a compatibility unique authority while
-- canonical assignment identity moves to (deployment_name, assignment_id).
UPDATE okg.coordination_native_state
   SET assignment_id = 'coordination:native:assignment:legacy_task:' || md5(
     octet_length(convert_to(coalesce(deployment_name, ''), 'UTF8'))::text ||
     ':' || coalesce(deployment_name, '') ||
     octet_length(convert_to(coalesce(change_id, ''), 'UTF8'))::text ||
     ':' || coalesce(change_id, '') ||
     octet_length(convert_to(coalesce(task_id, ''), 'UTF8'))::text ||
     ':' || coalesce(task_id, '')
   )
 WHERE assignment_id IS NULL;
UPDATE okg.coordination_native_state
   SET assignment_kind = 'legacy_task'
 WHERE assignment_kind IS NULL;
UPDATE okg.coordination_native_state
   SET assignment_generation = 1
 WHERE assignment_generation IS NULL OR assignment_generation <= 0;
UPDATE okg.coordination_native_state
   SET assignment_status = CASE
     WHEN claim_state = 'active' THEN 'active'
     ELSE 'retired'
   END
 WHERE assignment_status IS NULL;
ALTER TABLE okg.coordination_native_state
  ALTER COLUMN assignment_id SET NOT NULL;
ALTER TABLE okg.coordination_native_state
  ALTER COLUMN assignment_kind SET NOT NULL,
  ALTER COLUMN assignment_kind SET DEFAULT 'legacy_task';
ALTER TABLE okg.coordination_native_state
  ALTER COLUMN assignment_generation SET NOT NULL,
  ALTER COLUMN assignment_generation SET DEFAULT 1;
ALTER TABLE okg.coordination_native_state
  ALTER COLUMN assignment_status SET NOT NULL,
  ALTER COLUMN assignment_status SET DEFAULT 'active';
ALTER TABLE okg.coordination_native_state
  DROP CONSTRAINT IF EXISTS coordination_native_state_assignment_status_check;
DO $$
DECLARE
  _pk_name TEXT;
  _pk_columns TEXT[];
BEGIN
  SELECT c.conname, array_agg(a.attname::TEXT ORDER BY keys.ordinality)
    INTO _pk_name, _pk_columns
    FROM pg_constraint c
    CROSS JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS keys(attnum, ordinality)
    JOIN pg_attribute a
      ON a.attrelid = c.conrelid AND a.attnum = keys.attnum
   WHERE c.conrelid = 'okg.coordination_native_state'::regclass
     AND c.contype = 'p'
   GROUP BY c.conname;
  IF _pk_name IS NOT NULL AND _pk_columns IS DISTINCT FROM
     ARRAY['deployment_name', 'assignment_id']::TEXT[] THEN
    EXECUTE format('ALTER TABLE okg.coordination_native_state DROP CONSTRAINT %I', _pk_name);
  END IF;
  IF NOT EXISTS (
    SELECT 1
      FROM pg_constraint c
     WHERE c.conrelid = 'okg.coordination_native_state'::regclass
       AND c.contype = 'p'
       AND (
         SELECT array_agg(a.attname::TEXT ORDER BY keys.ordinality)
           FROM unnest(c.conkey) WITH ORDINALITY AS keys(attnum, ordinality)
           JOIN pg_attribute a
             ON a.attrelid = c.conrelid AND a.attnum = keys.attnum
       ) = ARRAY['deployment_name', 'assignment_id']::TEXT[]
  ) THEN
    ALTER TABLE okg.coordination_native_state
      ADD CONSTRAINT coordination_native_state_pkey PRIMARY KEY
      (deployment_name, assignment_id);
  END IF;
  IF EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conrelid = 'okg.coordination_native_state'::regclass
       AND conname = 'coordination_native_state_legacy_key'
  ) THEN
    ALTER TABLE okg.coordination_native_state
      DROP CONSTRAINT coordination_native_state_legacy_key;
  END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS coordination_native_state_legacy_key
  ON okg.coordination_native_state (deployment_name, change_id, task_id)
  WHERE assignment_kind = 'legacy_task'
    AND change_id IS NOT NULL
    AND task_id IS NOT NULL;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid =
    'okg.coordination_native_state'::regclass AND conname =
    'coordination_native_state_assignment_id_check') THEN
    ALTER TABLE okg.coordination_native_state ADD CONSTRAINT
      coordination_native_state_assignment_id_check
      CHECK (length(btrim(assignment_id)) > 0);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid =
    'okg.coordination_native_state'::regclass AND conname =
    'coordination_native_state_assignment_kind_check') THEN
    ALTER TABLE okg.coordination_native_state ADD CONSTRAINT
      coordination_native_state_assignment_kind_check CHECK
      (assignment_kind IN ('chief', 'sprint', 'pact', 'delegation', 'legacy_task'));
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid =
    'okg.coordination_native_state'::regclass AND conname =
    'coordination_native_state_assignment_generation_check') THEN
    ALTER TABLE okg.coordination_native_state ADD CONSTRAINT
      coordination_native_state_assignment_generation_check
      CHECK (assignment_generation > 0);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid =
    'okg.coordination_native_state'::regclass AND conname =
    'coordination_native_state_assignment_status_check') THEN
    ALTER TABLE okg.coordination_native_state ADD CONSTRAINT
      coordination_native_state_assignment_status_check
      CHECK (assignment_status IN ('pending', 'starting', 'active', 'retired', 'frozen'));
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid =
    'okg.coordination_native_state'::regclass AND conname =
    'coordination_native_state_assignment_owner_check') THEN
    ALTER TABLE okg.coordination_native_state ADD CONSTRAINT
      coordination_native_state_assignment_owner_check CHECK (
        assignment_kind = 'legacy_task'
        OR (
          owner_principal IS NOT NULL AND length(btrim(owner_principal)) > 0
          AND owner_session_id IS NOT NULL AND length(btrim(owner_session_id)) > 0
        )
      );
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid =
    'okg.coordination_native_state'::regclass AND conname =
    'coordination_native_state_execution_lane_check') THEN
    ALTER TABLE okg.coordination_native_state ADD CONSTRAINT
      coordination_native_state_execution_lane_check
      CHECK (execution_lane IS NULL OR execution_lane IN ('critical', 'light'));
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid =
    'okg.coordination_native_state'::regclass AND conname =
    'coordination_native_state_named_resource_demand_check') THEN
    ALTER TABLE okg.coordination_native_state ADD CONSTRAINT
      coordination_native_state_named_resource_demand_check
      CHECK (jsonb_typeof(named_resource_demand) = 'object');
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid =
    'okg.coordination_native_state'::regclass AND conname =
    'coordination_native_state_check') THEN
    ALTER TABLE okg.coordination_native_state ADD CONSTRAINT
      coordination_native_state_check
      CHECK (parent_assignment_id IS NULL OR parent_assignment_id <> assignment_id);
  END IF;
END $$;

ALTER TABLE okg.coordination_native_state
  ALTER COLUMN change_id DROP NOT NULL,
  ALTER COLUMN task_id DROP NOT NULL;

-- Server-owned enrollment for the existing restricted SSH coordination
-- route.  Agents can select only the opaque target_route; credential,
-- logical-host, peer, workspace, and start-plan bindings are installed by the
-- authenticated setup control and never accepted by the forced command.
CREATE TABLE IF NOT EXISTS okg.coordination_native_target_enrollments (
  deployment_name TEXT NOT NULL,
  target_route TEXT NOT NULL,
  route_generation BIGINT NOT NULL DEFAULT 1,
  route_digest TEXT NOT NULL,
  credential_id TEXT NOT NULL,
  owner_principal TEXT NOT NULL,
  owner_session_id TEXT NOT NULL,
  workspace_id TEXT NOT NULL,
  git_worktree_id TEXT NOT NULL,
  okg_branch_id TEXT NOT NULL,
  authenticated_host TEXT NOT NULL,
  source_address TEXT NOT NULL,
  source_port INTEGER,
  target_address TEXT NOT NULL,
  target_port INTEGER NOT NULL,
  start_plan JSONB NOT NULL,
  status TEXT NOT NULL DEFAULT 'enrolled',
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (deployment_name, target_route),
  UNIQUE (deployment_name, credential_id),
  CHECK (route_generation > 0),
  CHECK (length(btrim(target_route)) > 0),
  CHECK (length(btrim(route_digest)) > 0),
  CHECK (source_port IS NULL OR source_port BETWEEN 1 AND 65535),
  CHECK (target_port BETWEEN 1 AND 65535),
  CHECK (jsonb_typeof(start_plan) = 'object'),
  CHECK (status IN ('enrolled', 'retired'))
);
ALTER TABLE okg.coordination_native_target_enrollments
  ALTER COLUMN source_port DROP NOT NULL;

-- Project-scoped operator control is part of the native coordination ledger.
-- An absent/uninitialized row preserves migration compatibility until the
-- first authenticated operator bootstrap.
CREATE TABLE IF NOT EXISTS okg.coordination_native_control (
  deployment_name TEXT PRIMARY KEY,
  control_mode TEXT NOT NULL DEFAULT 'uninitialized',
  control_generation BIGINT NOT NULL DEFAULT 0,
  approved_plan JSONB,
  approved_plan_digest TEXT,
  policy_digest TEXT,
  project_digest TEXT,
  last_action TEXT,
  last_idempotency_key TEXT,
  last_request_digest TEXT,
  last_receipt JSONB,
  -- The setup handoff lives in this row so a process crash cannot erase a
  -- prepared plan or an already-activated paired workspace proof.
  bootstrap_pair_proof JSONB,
  bootstrap_pair_proof_digest TEXT,
  bootstrap_prepare_receipt JSONB,
  bootstrap_pair_receipt JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (control_mode IN ('uninitialized', 'preparing', 'open', 'frozen')),
  CHECK (control_generation >= 0),
  CHECK (
    (control_mode = 'uninitialized'
      AND control_generation = 0
      AND approved_plan IS NULL
      AND approved_plan_digest IS NULL
      AND policy_digest IS NULL
      AND project_digest IS NULL
      AND bootstrap_pair_proof IS NULL
      AND bootstrap_pair_proof_digest IS NULL)
    OR
    (control_mode = 'preparing'
      AND control_generation = 0
      AND approved_plan IS NOT NULL
      AND approved_plan_digest IS NOT NULL
      AND policy_digest IS NOT NULL
      AND project_digest IS NOT NULL
      AND bootstrap_prepare_receipt IS NOT NULL)
    OR
    (control_mode IN ('open', 'frozen')
      AND control_generation > 0
      AND approved_plan IS NOT NULL
      AND approved_plan_digest IS NOT NULL
      AND policy_digest IS NOT NULL
      AND project_digest IS NOT NULL)
  )
);

ALTER TABLE okg.coordination_native_control
  ADD COLUMN IF NOT EXISTS control_mode TEXT NOT NULL DEFAULT 'uninitialized',
  ADD COLUMN IF NOT EXISTS control_generation BIGINT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS approved_plan JSONB,
  ADD COLUMN IF NOT EXISTS approved_plan_digest TEXT,
  ADD COLUMN IF NOT EXISTS policy_digest TEXT,
  ADD COLUMN IF NOT EXISTS project_digest TEXT,
  ADD COLUMN IF NOT EXISTS last_action TEXT,
  ADD COLUMN IF NOT EXISTS last_idempotency_key TEXT,
  ADD COLUMN IF NOT EXISTS last_request_digest TEXT,
  ADD COLUMN IF NOT EXISTS last_receipt JSONB,
  ADD COLUMN IF NOT EXISTS bootstrap_pair_proof JSONB,
  ADD COLUMN IF NOT EXISTS bootstrap_pair_proof_digest TEXT,
  ADD COLUMN IF NOT EXISTS bootstrap_prepare_receipt JSONB,
  ADD COLUMN IF NOT EXISTS bootstrap_pair_receipt JSONB,
  ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

-- ``CREATE TABLE IF NOT EXISTS`` does not revise checks on an existing
-- installation.  Replace only the old control-state checks, idempotently,
-- so the new crash-visible preparing mode is accepted during migration.
DO $$
DECLARE
  _constraint RECORD;
BEGIN
  FOR _constraint IN
    SELECT conname
      FROM pg_constraint
     WHERE conrelid = 'okg.coordination_native_control'::regclass
       AND contype = 'c'
       AND (
         pg_get_constraintdef(oid) ILIKE '%control_mode%'
         OR pg_get_constraintdef(oid) ILIKE '%control_generation%'
         OR pg_get_constraintdef(oid) ILIKE '%approved_plan%'
       )
  LOOP
    EXECUTE format(
      'ALTER TABLE okg.coordination_native_control DROP CONSTRAINT %I',
      _constraint.conname
    );
  END LOOP;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid =
    'okg.coordination_native_control'::regclass AND conname =
    'coordination_native_control_mode_check') THEN
    ALTER TABLE okg.coordination_native_control
      ADD CONSTRAINT coordination_native_control_mode_check
      CHECK (control_mode IN ('uninitialized', 'preparing', 'open', 'frozen'));
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid =
    'okg.coordination_native_control'::regclass AND conname =
    'coordination_native_control_generation_check') THEN
    ALTER TABLE okg.coordination_native_control
      ADD CONSTRAINT coordination_native_control_generation_check
      CHECK (control_generation >= 0);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid =
    'okg.coordination_native_control'::regclass AND conname =
    'coordination_native_control_state_check') THEN
    ALTER TABLE okg.coordination_native_control
      ADD CONSTRAINT coordination_native_control_state_check
      CHECK (
        (control_mode = 'uninitialized'
          AND control_generation = 0
          AND approved_plan IS NULL
          AND approved_plan_digest IS NULL
          AND policy_digest IS NULL
          AND project_digest IS NULL
          AND bootstrap_pair_proof IS NULL
          AND bootstrap_pair_proof_digest IS NULL)
        OR
        (control_mode = 'preparing'
          AND control_generation = 0
          AND approved_plan IS NOT NULL
          AND approved_plan_digest IS NOT NULL
          AND policy_digest IS NOT NULL
          AND project_digest IS NOT NULL
          AND bootstrap_prepare_receipt IS NOT NULL)
        OR
        (control_mode IN ('open', 'frozen')
          AND control_generation > 0
          AND approved_plan IS NOT NULL
          AND approved_plan_digest IS NOT NULL
          AND policy_digest IS NOT NULL
          AND project_digest IS NOT NULL)
      );
  END IF;
END $$;

CREATE OR REPLACE FUNCTION okg.coordination_native_control_touch_updated_at()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS coordination_native_control_touch_updated_at
  ON okg.coordination_native_control;
CREATE TRIGGER coordination_native_control_touch_updated_at
BEFORE UPDATE ON okg.coordination_native_control
FOR EACH ROW
EXECUTE FUNCTION okg.coordination_native_control_touch_updated_at();

CREATE OR REPLACE FUNCTION okg.coordination_native_state_touch_updated_at()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS coordination_native_state_touch_updated_at
  ON okg.coordination_native_state;
CREATE TRIGGER coordination_native_state_touch_updated_at
BEFORE UPDATE ON okg.coordination_native_state
FOR EACH ROW
EXECUTE FUNCTION okg.coordination_native_state_touch_updated_at();

CREATE TABLE IF NOT EXISTS okg.coordination_native_events (
  event_id TEXT PRIMARY KEY,
  deployment_name TEXT NOT NULL,
  event_type TEXT NOT NULL,
  change_id TEXT,
  task_id TEXT,
  actor_id TEXT NOT NULL,
  actor_session_id TEXT NOT NULL,
  role_agent_id TEXT,
  workspace_id TEXT,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  observed_at TIMESTAMPTZ NOT NULL,
  idempotency_key TEXT NOT NULL,
  expected_revision BIGINT NOT NULL,
  resulting_revision BIGINT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('accepted', 'rejected')),
  rejection_code TEXT,
  rejection_reason TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  -- Keep fresh-install column order aligned with the append-only upgrade
  -- path below.  PostgreSQL preserves physical column order in schema dumps.
  assignment_id TEXT NOT NULL,
  assignment_kind TEXT NOT NULL DEFAULT 'legacy_task',
  assignment_generation BIGINT NOT NULL DEFAULT 1,
  assignment_status TEXT NOT NULL DEFAULT 'active',
  project_id TEXT,
  sprint_id TEXT,
  parent_assignment_id TEXT,
  owner_principal TEXT,
  owner_session_id TEXT,
  git_worktree_id TEXT,
  okg_branch_id TEXT,
  authenticated_host TEXT,
  credential_id TEXT,
  source_address TEXT,
  source_port INTEGER,
  target_address TEXT,
  target_port INTEGER,
  transport_verified_by TEXT,
  allowed_event_types JSONB NOT NULL DEFAULT '[]'::jsonb,
  lease_expires_at TIMESTAMPTZ,
  policy_digest TEXT,
  project_digest TEXT,
  execution_profile TEXT,
  execution_lane TEXT,
  named_resource_demand JSONB NOT NULL DEFAULT '{}'::jsonb,
  capacity_digest TEXT,
  request_digest TEXT,
  message_id TEXT,
  recipient_assignment_id TEXT,
  recipient_generation BIGINT,
  recipient_principal TEXT,
  recipient_session_id TEXT,
  parent_generation BIGINT,
  CHECK (length(btrim(assignment_id)) > 0),
  CHECK (assignment_kind IN ('chief', 'sprint', 'pact', 'delegation', 'legacy_task')),
  CHECK (assignment_generation > 0),
  CHECK (assignment_status IN ('pending', 'starting', 'active', 'retired', 'frozen')),
  CONSTRAINT coordination_native_events_assignment_owner_check CHECK (
    assignment_kind = 'legacy_task'
    OR (
      owner_principal IS NOT NULL AND length(btrim(owner_principal)) > 0
      AND owner_session_id IS NOT NULL AND length(btrim(owner_session_id)) > 0
    )
  )
);

ALTER TABLE okg.coordination_native_events
  ADD COLUMN IF NOT EXISTS assignment_id TEXT,
  ADD COLUMN IF NOT EXISTS assignment_kind TEXT,
  ADD COLUMN IF NOT EXISTS assignment_generation BIGINT,
  ADD COLUMN IF NOT EXISTS assignment_status TEXT,
  ADD COLUMN IF NOT EXISTS project_id TEXT,
  ADD COLUMN IF NOT EXISTS sprint_id TEXT,
  ADD COLUMN IF NOT EXISTS parent_assignment_id TEXT,
  ADD COLUMN IF NOT EXISTS owner_principal TEXT,
  ADD COLUMN IF NOT EXISTS owner_session_id TEXT,
  ADD COLUMN IF NOT EXISTS git_worktree_id TEXT,
  ADD COLUMN IF NOT EXISTS okg_branch_id TEXT,
  ADD COLUMN IF NOT EXISTS authenticated_host TEXT,
  ADD COLUMN IF NOT EXISTS credential_id TEXT,
  ADD COLUMN IF NOT EXISTS source_address TEXT,
  ADD COLUMN IF NOT EXISTS source_port INTEGER,
  ADD COLUMN IF NOT EXISTS target_address TEXT,
  ADD COLUMN IF NOT EXISTS target_port INTEGER,
  ADD COLUMN IF NOT EXISTS transport_verified_by TEXT,
  ADD COLUMN IF NOT EXISTS allowed_event_types JSONB NOT NULL DEFAULT '[]'::jsonb,
  ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS policy_digest TEXT,
  ADD COLUMN IF NOT EXISTS project_digest TEXT,
  ADD COLUMN IF NOT EXISTS execution_profile TEXT,
  ADD COLUMN IF NOT EXISTS execution_lane TEXT,
  ADD COLUMN IF NOT EXISTS named_resource_demand JSONB NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS capacity_digest TEXT,
  ADD COLUMN IF NOT EXISTS request_digest TEXT,
  ADD COLUMN IF NOT EXISTS message_id TEXT,
  ADD COLUMN IF NOT EXISTS recipient_assignment_id TEXT,
  ADD COLUMN IF NOT EXISTS recipient_generation BIGINT,
  ADD COLUMN IF NOT EXISTS recipient_principal TEXT,
  ADD COLUMN IF NOT EXISTS recipient_session_id TEXT,
  ADD COLUMN IF NOT EXISTS parent_generation BIGINT;
UPDATE okg.coordination_native_events
   SET assignment_id = 'coordination:native:assignment:legacy_task:' || md5(
     octet_length(convert_to(coalesce(deployment_name, ''), 'UTF8'))::text ||
     ':' || coalesce(deployment_name, '') ||
     octet_length(convert_to(coalesce(change_id, ''), 'UTF8'))::text ||
     ':' || coalesce(change_id, '') ||
     octet_length(convert_to(coalesce(task_id, ''), 'UTF8'))::text ||
     ':' || coalesce(task_id, '')
   )
 WHERE assignment_id IS NULL;
UPDATE okg.coordination_native_events
   SET assignment_kind = 'legacy_task'
 WHERE assignment_kind IS NULL;
UPDATE okg.coordination_native_events
   SET assignment_generation = 1
 WHERE assignment_generation IS NULL OR assignment_generation <= 0;
UPDATE okg.coordination_native_events
   SET assignment_status = 'active'
 WHERE assignment_status IS NULL;
ALTER TABLE okg.coordination_native_events
  ALTER COLUMN assignment_id SET NOT NULL;
ALTER TABLE okg.coordination_native_events
  ALTER COLUMN assignment_kind SET NOT NULL,
  ALTER COLUMN assignment_kind SET DEFAULT 'legacy_task';
ALTER TABLE okg.coordination_native_events
  ALTER COLUMN assignment_generation SET NOT NULL,
  ALTER COLUMN assignment_generation SET DEFAULT 1;
ALTER TABLE okg.coordination_native_events
  ALTER COLUMN assignment_status SET NOT NULL,
  ALTER COLUMN assignment_status SET DEFAULT 'active';
ALTER TABLE okg.coordination_native_events
  DROP CONSTRAINT IF EXISTS coordination_native_events_assignment_status_check;
ALTER TABLE okg.coordination_native_events
  ALTER COLUMN change_id DROP NOT NULL,
  ALTER COLUMN task_id DROP NOT NULL;

CREATE OR REPLACE FUNCTION okg.coordination_native_guarded_event_immutable()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF OLD.actor_id IN ('guarded-writer', 'operator:viz-token', 'coordination-policy')
     OR left(OLD.event_type, 11) = 'assignment_'
     OR OLD.event_type IN ('message_send', 'message_ack') THEN
    RAISE EXCEPTION 'protected coordination events are immutable';
  END IF;
  RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
END;
$$;

DROP TRIGGER IF EXISTS coordination_native_guarded_event_immutable
  ON okg.coordination_native_events;
CREATE TRIGGER coordination_native_guarded_event_immutable
BEFORE UPDATE OR DELETE ON okg.coordination_native_events
FOR EACH ROW
EXECUTE FUNCTION okg.coordination_native_guarded_event_immutable();

ALTER TABLE okg.coordination_native_events
  DROP CONSTRAINT IF EXISTS coordination_native_events_assignment_idempotency_key;

-- Replace the original task-shaped table constraint with a partial index.
-- The original constraint also caught guarded events whose change/task fields
-- were populated, which prevented a successor generation from reusing a key.
DO $$
DECLARE
  constraint_name TEXT;
BEGIN
  FOR constraint_name IN
    SELECT conname
      FROM pg_constraint
     WHERE conrelid = 'okg.coordination_native_events'::regclass
       AND contype = 'u'
       AND pg_get_constraintdef(oid) =
         'UNIQUE (deployment_name, actor_id, change_id, task_id, idempotency_key)'
  LOOP
    EXECUTE format(
      'ALTER TABLE okg.coordination_native_events DROP CONSTRAINT %I',
      constraint_name
    );
  END LOOP;
END $$;

DROP INDEX IF EXISTS okg.coordination_native_events_legacy_idempotency_uq;
CREATE UNIQUE INDEX coordination_native_events_legacy_idempotency_uq
  ON okg.coordination_native_events (
    deployment_name, actor_id, change_id, task_id, idempotency_key
  )
  WHERE actor_id NOT IN (
    'guarded-writer', 'operator:viz-token', 'assignment-handshake'
  );

DROP INDEX IF EXISTS okg.coordination_native_events_guarded_idempotency_uq;
CREATE UNIQUE INDEX coordination_native_events_guarded_idempotency_uq
  ON okg.coordination_native_events (
    deployment_name, assignment_id, assignment_generation, idempotency_key,
    (CASE WHEN rejection_code = 'approval_required'
          THEN event_id ELSE 'terminal' END)
  )
  WHERE actor_id = 'guarded-writer';

CREATE UNIQUE INDEX IF NOT EXISTS coordination_native_events_operator_idempotency_uq
  ON okg.coordination_native_events (deployment_name, idempotency_key)
  WHERE actor_id = 'operator:viz-token';

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid =
    'okg.coordination_native_events'::regclass AND conname =
    'coordination_native_events_assignment_id_check') THEN
    ALTER TABLE okg.coordination_native_events ADD CONSTRAINT
      coordination_native_events_assignment_id_check
      CHECK (length(btrim(assignment_id)) > 0);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid =
    'okg.coordination_native_events'::regclass AND conname =
    'coordination_native_events_assignment_kind_check') THEN
    ALTER TABLE okg.coordination_native_events ADD CONSTRAINT
      coordination_native_events_assignment_kind_check CHECK
      (assignment_kind IN ('chief', 'sprint', 'pact', 'delegation', 'legacy_task'));
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid =
    'okg.coordination_native_events'::regclass AND conname =
    'coordination_native_events_assignment_generation_check') THEN
    ALTER TABLE okg.coordination_native_events ADD CONSTRAINT
      coordination_native_events_assignment_generation_check
      CHECK (assignment_generation > 0);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid =
    'okg.coordination_native_events'::regclass AND conname =
    'coordination_native_events_assignment_status_check') THEN
    ALTER TABLE okg.coordination_native_events ADD CONSTRAINT
      coordination_native_events_assignment_status_check
      CHECK (assignment_status IN ('pending', 'starting', 'active', 'retired', 'frozen'));
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid =
    'okg.coordination_native_events'::regclass AND conname =
    'coordination_native_events_assignment_owner_check') THEN
    ALTER TABLE okg.coordination_native_events ADD CONSTRAINT
      coordination_native_events_assignment_owner_check CHECK (
        assignment_kind = 'legacy_task'
        OR (
          owner_principal IS NOT NULL AND length(btrim(owner_principal)) > 0
          AND owner_session_id IS NOT NULL AND length(btrim(owner_session_id)) > 0
        )
      );
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS coordination_native_events_task_idx
  ON okg.coordination_native_events
  (deployment_name, change_id, task_id, observed_at DESC);

CREATE INDEX IF NOT EXISTS coordination_native_events_status_idx
  ON okg.coordination_native_events
  (deployment_name, status, event_type, observed_at DESC);

CREATE INDEX IF NOT EXISTS coordination_native_state_workspace_idx
  ON okg.coordination_native_state
  (deployment_name, workspace_id)
  WHERE workspace_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS coordination_native_state_credential_idx
  ON okg.coordination_native_state (deployment_name, credential_id, assignment_id)
  WHERE credential_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS coordination_native_state_live_chief_project_uq
  ON okg.coordination_native_state (deployment_name, project_id)
  WHERE assignment_kind = 'chief'
    AND assignment_status = 'active'
    AND project_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS coordination_native_state_live_sprint_uq
  ON okg.coordination_native_state (deployment_name, sprint_id)
  WHERE assignment_kind = 'sprint'
    AND assignment_status = 'active'
    AND sprint_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS coordination_native_state_live_pact_change_uq
  ON okg.coordination_native_state (deployment_name, change_id)
  WHERE assignment_kind = 'pact'
    AND assignment_status = 'active'
    AND change_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS coordination_native_state_live_pact_owner_uq
  ON okg.coordination_native_state (
    deployment_name, owner_principal, owner_session_id
  )
  WHERE assignment_kind = 'pact'
    AND assignment_status = 'active'
    AND owner_principal IS NOT NULL
    AND owner_session_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS coordination_native_state_live_sprint_owner_uq
  ON okg.coordination_native_state (
    deployment_name, owner_principal, owner_session_id
  )
  WHERE assignment_kind = 'sprint'
    AND assignment_status = 'active'
    AND owner_principal IS NOT NULL
    AND owner_session_id IS NOT NULL;

CREATE OR REPLACE FUNCTION okg.coordination_native_assignment_guard()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF NEW.deployment_name <> OLD.deployment_name
     OR NEW.assignment_id <> OLD.assignment_id
     OR NEW.assignment_kind <> OLD.assignment_kind
     OR NEW.project_id IS DISTINCT FROM OLD.project_id
     OR NEW.sprint_id IS DISTINCT FROM OLD.sprint_id
     OR NEW.change_id IS DISTINCT FROM OLD.change_id
     OR NEW.task_id IS DISTINCT FROM OLD.task_id
     OR NEW.parent_assignment_id IS DISTINCT FROM OLD.parent_assignment_id THEN
    RAISE EXCEPTION 'assignment identity is immutable';
  END IF;
  IF NEW.assignment_generation < OLD.assignment_generation THEN
    RAISE EXCEPTION 'assignment_generation cannot decrease';
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS coordination_native_assignment_guard
  ON okg.coordination_native_state;
CREATE TRIGGER coordination_native_assignment_guard
BEFORE UPDATE ON okg.coordination_native_state
FOR EACH ROW
EXECUTE FUNCTION okg.coordination_native_assignment_guard();

-- Enforce the first-level assignment hierarchy for direct database writers as
-- well as the Python writer.  The regular application roles are revoked below,
-- but this trigger keeps a privileged/raw INSERT from creating an impossible
-- active parent or widening a delegation.  Legacy task rows remain a
-- compatibility projection and deliberately bypass these generic checks.
CREATE OR REPLACE FUNCTION okg.coordination_native_assignment_hierarchy_guard()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  parent_row RECORD;
  existing_kind TEXT;
BEGIN
  IF TG_OP = 'DELETE' THEN
    IF OLD.assignment_kind <> 'legacy_task' THEN
      RAISE EXCEPTION
        'generic assignment rows are immutable; retire the assignment instead';
    END IF;
    RETURN OLD;
  END IF;

  IF TG_OP = 'UPDATE'
     AND OLD.assignment_status = 'active'
     AND NEW.assignment_status = 'retired' THEN
    IF OLD.assignment_kind = 'chief' THEN
      PERFORM assignment_id
        FROM okg.coordination_native_state
       WHERE deployment_name = OLD.deployment_name
         AND assignment_kind = 'sprint'
         AND assignment_status = 'active'
         AND project_id = OLD.project_id
       LIMIT 1
       FOR UPDATE;
      IF FOUND THEN
        RAISE EXCEPTION
          'cannot retire chief: active sprint children remain';
      END IF;
    ELSIF OLD.assignment_kind = 'sprint' THEN
      PERFORM assignment_id
        FROM okg.coordination_native_state
       WHERE deployment_name = OLD.deployment_name
         AND assignment_kind = 'pact'
         AND assignment_status = 'active'
         AND project_id = OLD.project_id
         AND sprint_id = OLD.sprint_id
       LIMIT 1
       FOR UPDATE;
      IF FOUND THEN
        RAISE EXCEPTION
          'cannot retire sprint: active PACT children remain';
      END IF;
    END IF;
    IF OLD.assignment_kind IN ('chief', 'sprint', 'pact') THEN
      PERFORM assignment_id
        FROM okg.coordination_native_state
       WHERE deployment_name = OLD.deployment_name
         AND parent_assignment_id = OLD.assignment_id
         AND assignment_kind = 'delegation'
         AND assignment_status = 'active'
       LIMIT 1
       FOR UPDATE;
      IF FOUND THEN
        RAISE EXCEPTION
          'cannot retire %: active delegation children remain',
          OLD.assignment_kind;
      END IF;
    END IF;
  END IF;

  IF NEW.assignment_kind = 'legacy_task'
     OR NEW.assignment_status <> 'active' THEN
    RETURN NEW;
  END IF;

  IF NEW.assignment_kind IN ('chief', 'sprint', 'pact')
     AND NEW.parent_assignment_id IS NOT NULL THEN
    RAISE EXCEPTION
      'only delegation assignments may have a parent assignment';
  END IF;

  IF NEW.assignment_kind = 'pact' AND NEW.task_id IS NOT NULL THEN
    RAISE EXCEPTION
      'PACT assignments are change-scoped; task_id belongs to a delegation';
  END IF;

  IF NEW.assignment_kind IN ('chief', 'sprint')
     AND NEW.execution_profile IS NOT NULL THEN
    RAISE EXCEPTION 'chief and sprint assignments must not declare execution_profile';
  END IF;
  IF NEW.assignment_kind IN ('pact', 'delegation')
     AND NEW.execution_profile IS NULL THEN
    RAISE EXCEPTION 'PACT and delegation assignments require execution_profile';
  END IF;
  IF NEW.assignment_kind IN ('pact', 'delegation')
     AND (NEW.execution_lane IS NULL OR NEW.capacity_digest IS NULL) THEN
    RAISE EXCEPTION 'execution lane and capacity digest are native-derived';
  END IF;
  IF NEW.execution_lane IS NOT NULL
     AND NEW.execution_lane NOT IN ('critical', 'light') THEN
    RAISE EXCEPTION 'execution_lane must be critical or light';
  END IF;
  IF jsonb_typeof(NEW.named_resource_demand) <> 'object' THEN
    RAISE EXCEPTION 'named_resource_demand must be an object';
  END IF;

  IF NEW.assignment_kind <> 'legacy_task'
     AND jsonb_typeof(NEW.allowed_event_types) <> 'array' THEN
    RAISE EXCEPTION 'assignment allowed_event_types must be an array';
  END IF;

  IF TG_OP = 'UPDATE'
     AND OLD.assignment_kind IN ('chief', 'sprint', 'pact')
     AND NEW.allowed_event_types IS DISTINCT FROM OLD.allowed_event_types THEN
    PERFORM assignment_id
      FROM okg.coordination_native_state
     WHERE deployment_name = OLD.deployment_name
       AND parent_assignment_id = OLD.assignment_id
       AND assignment_kind = 'delegation'
       AND assignment_status = 'active'
       AND NOT (allowed_event_types <@ NEW.allowed_event_types)
     LIMIT 1
     FOR UPDATE;
    IF FOUND THEN
      RAISE EXCEPTION
        'parent allowed_event_types cannot become narrower than an active delegation';
    END IF;
  END IF;

  IF NEW.assignment_kind IN ('chief', 'sprint', 'pact')
     AND NEW.owner_principal IS NOT NULL
     AND NEW.owner_session_id IS NOT NULL THEN
    PERFORM pg_advisory_xact_lock(hashtext(
      'coord-durable-owner:' || NEW.deployment_name || ':' ||
      NEW.owner_principal || ':' || NEW.owner_session_id
    ));
    SELECT assignment_kind INTO existing_kind
      FROM okg.coordination_native_state
     WHERE deployment_name = NEW.deployment_name
       AND assignment_status = 'active'
       AND assignment_kind IN ('chief', 'sprint', 'pact')
       AND assignment_kind <> NEW.assignment_kind
       AND owner_principal = NEW.owner_principal
       AND owner_session_id = NEW.owner_session_id
       AND assignment_id <> NEW.assignment_id
     LIMIT 1;
    IF existing_kind IS NOT NULL THEN
      RAISE EXCEPTION
        'principal/session already holds an active % assignment', existing_kind;
    END IF;
  END IF;

  IF NEW.assignment_kind = 'sprint' THEN
    IF NEW.project_id IS NULL OR NEW.sprint_id IS NULL THEN
      RAISE EXCEPTION 'sprint assignments require project_id and sprint_id';
    END IF;
    PERFORM assignment_id
      FROM okg.coordination_native_state
     WHERE deployment_name = NEW.deployment_name
       AND assignment_kind = 'chief'
       AND assignment_status = 'active'
       AND project_id = NEW.project_id
     LIMIT 1
     FOR SHARE;
    IF NOT FOUND THEN
      RAISE EXCEPTION
        'sprint project_id must match an active chief assignment';
    END IF;
  ELSIF NEW.assignment_kind = 'chief' THEN
    IF NEW.project_id IS NULL THEN
      RAISE EXCEPTION 'chief assignments require project_id';
    END IF;
  ELSIF NEW.assignment_kind = 'pact' THEN
    IF NEW.change_id IS NULL THEN
      RAISE EXCEPTION 'PACT assignments require change_id';
    END IF;
    IF NEW.project_id IS NULL OR NEW.sprint_id IS NULL THEN
      RAISE EXCEPTION 'PACT assignments require project_id and sprint_id';
    END IF;
    PERFORM assignment_id
      FROM okg.coordination_native_state
     WHERE deployment_name = NEW.deployment_name
       AND assignment_kind = 'sprint'
       AND assignment_status = 'active'
       AND project_id = NEW.project_id
       AND sprint_id = NEW.sprint_id
     LIMIT 1
     FOR SHARE;
    IF NOT FOUND THEN
      RAISE EXCEPTION
        'PACT project_id and sprint_id must match an active sprint assignment';
    END IF;
  ELSIF NEW.assignment_kind = 'delegation' THEN
    SELECT * INTO parent_row
      FROM okg.coordination_native_state
     WHERE deployment_name = NEW.deployment_name
       AND assignment_id = NEW.parent_assignment_id
     FOR SHARE;
    IF NOT FOUND OR parent_row.assignment_status <> 'active'
       OR parent_row.assignment_kind NOT IN ('chief', 'sprint', 'pact') THEN
      RAISE EXCEPTION
        'delegation parent must be an active chief, sprint, or PACT assignment';
    END IF;
    IF NEW.git_worktree_id IS NOT NULL OR NEW.okg_branch_id IS NOT NULL THEN
      RAISE EXCEPTION
        'delegation assignments cannot receive a separate worktree or OKG branch';
    END IF;
    IF NEW.project_id IS DISTINCT FROM parent_row.project_id
       OR NEW.sprint_id IS DISTINCT FROM parent_row.sprint_id
       OR NEW.change_id IS DISTINCT FROM parent_row.change_id THEN
      RAISE EXCEPTION
        'delegation project, sprint, and change scope must be inherited from its parent';
    END IF;
    IF jsonb_typeof(NEW.allowed_event_types) <> 'array'
       OR jsonb_typeof(parent_row.allowed_event_types) <> 'array'
       OR NOT (NEW.allowed_event_types <@ parent_row.allowed_event_types) THEN
      RAISE EXCEPTION
        'delegation allowed_event_types must be a subset of its parent';
    END IF;
  END IF;

  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS coordination_native_assignment_hierarchy_guard
  ON okg.coordination_native_state;
CREATE TRIGGER coordination_native_assignment_hierarchy_guard
BEFORE INSERT OR UPDATE OR DELETE ON okg.coordination_native_state
FOR EACH ROW
EXECUTE FUNCTION okg.coordination_native_assignment_hierarchy_guard();

CREATE INDEX IF NOT EXISTS coordination_native_state_updated_idx
  ON okg.coordination_native_state
  (deployment_name, updated_at DESC, work_item_id);

CREATE INDEX IF NOT EXISTS coordination_native_events_created_idx
  ON okg.coordination_native_events
  (deployment_name, created_at DESC, event_id);

CREATE INDEX IF NOT EXISTS coordination_native_events_credential_idx
  ON okg.coordination_native_events (deployment_name, credential_id, created_at DESC)
  WHERE credential_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS okg.coordination_native_messages (
  message_id TEXT PRIMARY KEY,
  deployment_name TEXT NOT NULL,
  sender_assignment_id TEXT NOT NULL,
  sender_generation BIGINT NOT NULL,
  sender_principal TEXT NOT NULL DEFAULT 'assignment-store',
  sender_session_id TEXT NOT NULL DEFAULT 'assignment-store',
  recipient_assignment_id TEXT NOT NULL,
  recipient_generation BIGINT NOT NULL,
  recipient_principal TEXT NOT NULL,
  recipient_session_id TEXT NOT NULL,
  parent_assignment_id TEXT,
  parent_generation BIGINT,
  kind TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  request_digest TEXT NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  status TEXT NOT NULL DEFAULT 'pending',
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  acknowledged_at TIMESTAMPTZ,
  ack_event_id TEXT,
  CHECK (sender_generation > 0),
  CHECK (recipient_generation > 0),
  CHECK (parent_generation IS NULL OR parent_generation > 0),
  CHECK (length(btrim(kind)) > 0),
  CHECK (length(btrim(idempotency_key)) > 0),
  CHECK (jsonb_typeof(payload) = 'object'),
  CHECK (octet_length(payload::text) <= 16384),
  CHECK (status IN ('pending', 'acknowledged')),
  UNIQUE (deployment_name, sender_assignment_id, sender_generation, idempotency_key)
);

ALTER TABLE okg.coordination_native_messages
  ADD COLUMN IF NOT EXISTS sender_principal TEXT NOT NULL DEFAULT 'assignment-store',
  ADD COLUMN IF NOT EXISTS sender_session_id TEXT NOT NULL DEFAULT 'assignment-store';

CREATE OR REPLACE FUNCTION okg.coordination_native_message_guard()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF TG_OP = 'DELETE' THEN
    RAISE EXCEPTION
      'coordination messages are immutable; acknowledge through the guarded writer';
  END IF;
  IF OLD.status <> 'pending'
     OR NEW.status <> 'acknowledged'
     OR NEW.acknowledged_at IS NULL
     OR NEW.ack_event_id IS NULL
     OR length(btrim(NEW.ack_event_id)) = 0
     OR (to_jsonb(NEW) - ARRAY['status', 'acknowledged_at', 'ack_event_id'])
        IS DISTINCT FROM
        (to_jsonb(OLD) - ARRAY['status', 'acknowledged_at', 'ack_event_id']) THEN
    RAISE EXCEPTION
      'coordination message fields are immutable; only exact acknowledgement is allowed';
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS coordination_native_message_guard
  ON okg.coordination_native_messages;
CREATE TRIGGER coordination_native_message_guard
BEFORE UPDATE OR DELETE ON okg.coordination_native_messages
FOR EACH ROW
EXECUTE FUNCTION okg.coordination_native_message_guard();

CREATE INDEX IF NOT EXISTS coordination_native_messages_recipient_idx
  ON okg.coordination_native_messages
  (deployment_name, recipient_assignment_id, recipient_generation, status);

-- One current row per exact authorization digest.  This is intentionally a
-- native coordination relation: PACT review decisions and message
-- acknowledgements are not approval state.  Retry keys are indexed only to
-- detect a same-key/different-bytes refusal; request_digest is the coalescing
-- authority across reconnects and retry keys.
CREATE TABLE IF NOT EXISTS okg.coordination_native_approvals (
  deployment_name TEXT NOT NULL,
  request_digest TEXT NOT NULL,
  approval_id TEXT NOT NULL,
  requester_assignment_id TEXT NOT NULL,
  requester_generation BIGINT NOT NULL,
  requester_principal TEXT NOT NULL,
  requester_session_id TEXT NOT NULL,
  assignment_kind TEXT NOT NULL,
  project_id TEXT,
  sprint_id TEXT,
  change_id TEXT,
  task_id TEXT,
  mandate TEXT NOT NULL,
  action TEXT NOT NULL,
  action_digest TEXT NOT NULL,
  resource_digest TEXT NOT NULL,
  scope_digest TEXT NOT NULL,
  human_policy_digest TEXT NOT NULL,
  route_worker_assignment_id TEXT,
  route_worker_generation BIGINT,
  route_worker_principal TEXT,
  route_worker_session_id TEXT,
  route_coordinator_assignment_id TEXT,
  route_coordinator_generation BIGINT,
  route_coordinator_principal TEXT,
  route_coordinator_session_id TEXT,
  route_chief_assignment_id TEXT,
  route_chief_generation BIGINT,
  route_chief_principal TEXT,
  route_chief_session_id TEXT,
  route_chain_digest TEXT NOT NULL DEFAULT '',
  route_state TEXT NOT NULL DEFAULT 'pending',
  blocker_code TEXT,
  status TEXT NOT NULL DEFAULT 'pending',
  decision TEXT,
  decided_by_principal TEXT,
  decided_by_session_id TEXT,
  retry_key TEXT NOT NULL,
  retry_request_digest TEXT NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL,
  max_uses INTEGER NOT NULL DEFAULT 1,
  uses INTEGER NOT NULL DEFAULT 0,
  revoked_at TIMESTAMPTZ,
  consumed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (deployment_name, request_digest),
  UNIQUE (deployment_name, approval_id),
  CHECK (length(btrim(request_digest)) > 0),
  CHECK (length(btrim(approval_id)) > 0),
  CHECK (requester_generation > 0),
  CHECK (route_worker_generation IS NULL OR route_worker_generation > 0),
  CHECK (route_coordinator_generation IS NULL OR route_coordinator_generation > 0),
  CHECK (route_chief_generation IS NULL OR route_chief_generation > 0),
  CHECK (status IN ('pending', 'granted', 'denied', 'revoked', 'consumed')),
  CHECK (route_state IN ('pending', 'routed', 'chief_unavailable', 'retired_route')),
  CHECK (max_uses = 1),
  CHECK (uses >= 0 AND uses <= max_uses),
  CHECK (status <> 'granted' OR decision = 'allow'),
  CHECK (status <> 'denied' OR decision = 'deny'),
  CHECK (status <> 'pending' OR decision IS NULL),
  CHECK (status NOT IN ('granted', 'denied') OR decided_by_principal IS NOT NULL),
  CHECK (retry_request_digest = request_digest)
);

ALTER TABLE okg.coordination_native_approvals
  ADD COLUMN IF NOT EXISTS route_worker_principal TEXT,
  ADD COLUMN IF NOT EXISTS route_worker_session_id TEXT,
  ADD COLUMN IF NOT EXISTS route_coordinator_principal TEXT,
  ADD COLUMN IF NOT EXISTS route_coordinator_session_id TEXT,
  ADD COLUMN IF NOT EXISTS route_chief_principal TEXT,
  ADD COLUMN IF NOT EXISTS route_chief_session_id TEXT,
  ADD COLUMN IF NOT EXISTS route_chain_digest TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS coordination_native_approvals_status_idx
  ON okg.coordination_native_approvals
  (deployment_name, status, updated_at DESC);
CREATE INDEX IF NOT EXISTS coordination_native_approvals_route_idx
  ON okg.coordination_native_approvals
  (deployment_name, route_chief_assignment_id, route_state, status);
CREATE UNIQUE INDEX IF NOT EXISTS coordination_native_approvals_retry_idx
  ON okg.coordination_native_approvals (
    deployment_name, requester_assignment_id, requester_generation, retry_key
  );

CREATE OR REPLACE FUNCTION okg.coordination_native_approvals_touch_updated_at()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at = clock_timestamp();
  RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS coordination_native_approvals_touch_updated_at
  ON okg.coordination_native_approvals;
CREATE TRIGGER coordination_native_approvals_touch_updated_at
BEFORE UPDATE ON okg.coordination_native_approvals
FOR EACH ROW EXECUTE FUNCTION okg.coordination_native_approvals_touch_updated_at();

DROP VIEW IF EXISTS okg.coordination_native_capacity_status;
CREATE VIEW okg.coordination_native_capacity_status AS
WITH active_reservations AS (
  SELECT deployment_name, execution_lane, named_resource_demand
    FROM okg.coordination_native_state
   WHERE assignment_kind IN ('pact', 'delegation')
     AND assignment_status IN ('pending', 'starting', 'active')
     AND (
       assignment_status = 'active'
       OR lease_expires_at > clock_timestamp()
     )
), lane_usage AS (
  SELECT deployment_name, 'lane'::text AS capacity_kind,
         execution_lane AS capacity_name, count(*)::bigint AS units
    FROM active_reservations
   WHERE execution_lane IS NOT NULL
   GROUP BY deployment_name, execution_lane
), resource_usage AS (
  SELECT reservation.deployment_name,
         'named_resource'::text AS capacity_kind,
         resource.key AS capacity_name,
         sum(
           CASE WHEN resource.value ~ '^[0-9]+$'
                THEN resource.value::bigint ELSE 0 END
         )::bigint AS units
    FROM active_reservations AS reservation
   CROSS JOIN LATERAL jsonb_each_text(
     reservation.named_resource_demand
   ) AS resource
   GROUP BY reservation.deployment_name, resource.key
)
SELECT * FROM lane_usage
UNION ALL
SELECT * FROM resource_usage;

DO $coordination_native_message_privileges$
DECLARE
  role_name TEXT;
BEGIN
  FOREACH role_name IN ARRAY ARRAY['app_rw', 'app_ro', 'okg_mcp', 'general_mcp'] LOOP
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role_name) THEN
      EXECUTE format(
        'REVOKE INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON '
        'okg.coordination_native_messages FROM %I', role_name
      );
    END IF;
  END LOOP;
END
$coordination_native_message_privileges$;

DO $coordination_native_approval_privileges$
DECLARE
  role_name TEXT;
BEGIN
  FOREACH role_name IN ARRAY ARRAY['app_rw', 'app_ro', 'okg_mcp', 'general_mcp'] LOOP
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role_name) THEN
      EXECUTE format(
        'REVOKE INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON '
        'okg.coordination_native_approvals FROM %I', role_name
      );
    END IF;
  END LOOP;
END
$coordination_native_approval_privileges$;

-- Assignment state is never a general application/MCP write surface. The
-- restricted writer owns the transaction above; these explicit revokes also
-- converge older databases whose broad grants predate generic assignments.
DO $coordination_native_assignment_privileges$
DECLARE
  role_name TEXT;
BEGIN
  FOREACH role_name IN ARRAY ARRAY['app_rw', 'app_ro', 'okg_mcp', 'general_mcp'] LOOP
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role_name) THEN
      EXECUTE format(
        'REVOKE INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON '
        'okg.coordination_native_state, okg.coordination_native_events, '
        'okg.coordination_native_control FROM %I',
        role_name
      );
    END IF;
  END LOOP;
END
$coordination_native_assignment_privileges$;

DO $$
DECLARE
  _constraint_name TEXT;
BEGIN
  SELECT conname
    INTO _constraint_name
    FROM pg_constraint
   WHERE conrelid = 'okg.admission_envelopes'::regclass
     AND contype = 'c'
     AND pg_get_constraintdef(oid) LIKE '%producer_kind%'
   LIMIT 1;
  IF _constraint_name IS NOT NULL THEN
    EXECUTE format(
      'ALTER TABLE okg.admission_envelopes DROP CONSTRAINT %I',
      _constraint_name
    );
  END IF;
  ALTER TABLE okg.admission_envelopes
    ADD CONSTRAINT admission_envelopes_producer_kind_check
    CHECK (producer_kind IN (
      'source', 'extractor', 'enricher', 'agent',
      'human', 'workflow', 'system', 'lens'
    ));

  -- Mirror the producer_kind widening on okg.producer_profiles so
  -- a materialized `_lens:<def_id>` profile passes the CHECK.
  SELECT conname
    INTO _constraint_name
    FROM pg_constraint
   WHERE conrelid = 'okg.producer_profiles'::regclass
     AND contype = 'c'
     AND pg_get_constraintdef(oid) LIKE '%producer_kind%'
   LIMIT 1;
  IF _constraint_name IS NOT NULL THEN
    EXECUTE format(
      'ALTER TABLE okg.producer_profiles DROP CONSTRAINT %I',
      _constraint_name
    );
  END IF;
  ALTER TABLE okg.producer_profiles
    ADD CONSTRAINT producer_profiles_producer_kind_check
    CHECK (producer_kind IN (
      'source', 'extractor', 'enricher', 'agent',
      'human', 'workflow', 'system', 'lens'
    ));
END
$$;

-- Durable bridge between bounded FactBatch chunks, Chronos graph application,
-- progress markers, and the optional audit fact log. The receipt identity and
-- canonical digest are immutable; application/recovery code advances only the
-- state and evidence columns.
CREATE TABLE IF NOT EXISTS okg.fact_batch_receipts (
  admission_id        UUID NOT NULL
    REFERENCES okg.admission_envelopes(admission_id) ON DELETE CASCADE,
  run_id              UUID NOT NULL,
  chunk_ordinal       INTEGER NOT NULL CHECK (chunk_ordinal >= 0),
  canonical_digest    TEXT NOT NULL
    CHECK (canonical_digest ~ '^sha256:[0-9a-f]{64}$'),
  target_branch       TEXT NOT NULL CHECK (btrim(target_branch) <> ''),
  state               TEXT NOT NULL
    CHECK (state IN ('prepared', 'applied', 'compensating', 'compensated')),
  marker_digest       TEXT NOT NULL
    CHECK (marker_digest ~ '^sha256:[0-9a-f]{64}$'),
  graph_result        JSONB NOT NULL DEFAULT '{}'::jsonb,
  branch_token        TEXT,
  audit_fact_ranges   JSONB NOT NULL DEFAULT '[]'::jsonb,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (admission_id, run_id, chunk_ordinal)
);

CREATE INDEX IF NOT EXISTS fact_batch_receipts_recovery_idx
  ON okg.fact_batch_receipts (state, updated_at)
  WHERE state IN ('prepared', 'compensating');

CREATE TABLE IF NOT EXISTS okg.admission_events (
  event_id            BIGSERIAL PRIMARY KEY,
  admission_id        UUID NOT NULL
    REFERENCES okg.admission_envelopes(admission_id) ON DELETE CASCADE,
  event_type          TEXT NOT NULL
    CHECK (event_type IN (
      'created', 'duplicate_coalesced', 'submitted',
      'accepted', 'rejected', 'quarantined',
      'published', 'blocked', 'failed', 'refs_attached'
    )),
  actor               TEXT,
  reason              TEXT,
  payload             JSONB NOT NULL DEFAULT '{}'::jsonb,
  workflow_ref        JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS admission_events_admission_created_idx
  ON okg.admission_events (admission_id, created_at DESC);
CREATE INDEX IF NOT EXISTS admission_events_type_idx
  ON okg.admission_events (event_type, created_at DESC);

CREATE TABLE IF NOT EXISTS okg.admission_record_refs (
  ref_id              BIGSERIAL PRIMARY KEY,
  admission_id        UUID NOT NULL
    REFERENCES okg.admission_envelopes(admission_id) ON DELETE CASCADE,
  ref_key             TEXT NOT NULL,
  ref_kind            TEXT NOT NULL
    CHECK (ref_kind IN (
      'node_fact_range', 'edge_fact_range', 'payload'
    )),
  source              TEXT,
  run_id              UUID,
  fact_id_min         BIGINT,
  fact_id_max         BIGINT,
  record_count        BIGINT NOT NULL DEFAULT 0
    CHECK (record_count >= 0),
  payload             JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (admission_id, ref_key)
);

CREATE INDEX IF NOT EXISTS admission_record_refs_admission_idx
  ON okg.admission_record_refs (admission_id, ref_kind);
CREATE INDEX IF NOT EXISTS admission_record_refs_source_run_idx
  ON okg.admission_record_refs (source, run_id)
  WHERE run_id IS NOT NULL;

-- Retention + compression policies. Community-edition Timescale.
-- Policies are idempotent — repeated add_*_policy calls with the
-- same parameters are no-ops.
SELECT add_retention_policy('okg.node_facts', INTERVAL '180 days', if_not_exists => TRUE);
SELECT add_retention_policy('okg.edge_facts', INTERVAL '180 days', if_not_exists => TRUE);

ALTER TABLE okg.node_facts SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'node_id',
  timescaledb.compress_orderby   = 'observed_at DESC'
);
ALTER TABLE okg.edge_facts SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'src, edge_type',
  timescaledb.compress_orderby   = 'observed_at DESC'
);

SELECT add_compression_policy('okg.node_facts', INTERVAL '30 days', if_not_exists => TRUE);
SELECT add_compression_policy('okg.edge_facts', INTERVAL '30 days', if_not_exists => TRUE);

-- ========== Generation catalog ==========

CREATE TABLE IF NOT EXISTS okg.graph_generations (
  generation_id       BIGSERIAL PRIMARY KEY,
  -- deployment_name is required (NOT NULL after backfill below);
  -- the publisher's advisory lock is per-deployment, so the
  -- generation row that the lock protects must also be scoped
  -- per-deployment. Without this column, publisher A on deployment
  -- "foo" would silently transition publisher B's pending row on
  -- "bar" to blocked=abandoned. Per Dijkstra M1 review #1.
  deployment_name     TEXT,
  status              TEXT      NOT NULL
    CHECK (status IN ('pending','published','blocked')),
  -- fact_watermark tracks node_facts.fact_id; edge_fact_watermark
  -- tracks edge_facts.fact_id (M5.5 — needed so retraction
  -- projection can bound its scan of op='R' facts per cycle).
  fact_watermark      BIGINT,
  edge_fact_watermark BIGINT,
  catalog_version_id  BIGINT    NOT NULL
    REFERENCES okg.catalog_versions(catalog_version_id),
  published_at        TIMESTAMPTZ,
  invariant_summary   JSONB     NOT NULL DEFAULT '{}'::jsonb,
  excluded_sources    JSONB     NOT NULL DEFAULT '[]'::jsonb,
  blocked_reason      JSONB,
  correlation_id      TEXT      NOT NULL,
  trace_id            TEXT,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Backfill existing rows + tighten the constraint. Idempotent:
-- a fresh DB with no rows passes; an upgraded DB with rows lacking
-- deployment_name has them stamped 'unknown' before NOT NULL fires.
UPDATE okg.graph_generations SET deployment_name = 'unknown'
  WHERE deployment_name IS NULL;
ALTER TABLE okg.graph_generations
  ALTER COLUMN deployment_name SET NOT NULL;

-- M5.5 added edge_fact_watermark. Idempotent upgrade for
-- pre-M5.5 schemas.
ALTER TABLE okg.graph_generations
  ADD COLUMN IF NOT EXISTS edge_fact_watermark BIGINT;

-- Unified telemetry correlation. Nullable for historical rows and
-- synthetic test generations; live publisher reservations stamp it.
ALTER TABLE okg.graph_generations
  ADD COLUMN IF NOT EXISTS trace_id TEXT;
CREATE INDEX IF NOT EXISTS graph_generations_trace_id_idx
  ON okg.graph_generations (trace_id)
  WHERE trace_id IS NOT NULL;

-- `add-delta-proportional-substrate-work` (2026-05-12): persist
-- per-generation projection deltas so read-side tools (bench,
-- viz, CLI) can answer "what changed in gen X?" via an indexed
-- single-row read instead of counting the generation's rows out of
-- graph storage on every call.
-- Populated by the publisher at commit time from
-- `PhaseAResult.delta`. Default 0 on existing rows — we don't
-- backfill historical deltas; the forward invariant only requires
-- new generations carry truthful counts.
ALTER TABLE okg.graph_generations
  ADD COLUMN IF NOT EXISTS nodes_added     BIGINT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS nodes_retracted BIGINT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS edges_added     BIGINT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS edges_retracted BIGINT NOT NULL DEFAULT 0;

-- Nomos policy pins on generations. These are populated at publish
-- time from active Nomos policy activations for the deployment; the
-- array default keeps pre-Nomos and bootstrap rows explicit rather
-- than NULL/unknown.
ALTER TABLE okg.graph_generations
  ADD COLUMN IF NOT EXISTS nomos_policy_bundle_ids TEXT[] NOT NULL
    DEFAULT ARRAY[]::TEXT[],
  ADD COLUMN IF NOT EXISTS nomos_policy_fingerprints TEXT[] NOT NULL
    DEFAULT ARRAY[]::TEXT[];

-- `add-delta-proportional-substrate-work` Change 3: extend the
-- status enum to include `no_op` — a cycle that reserved a
-- generation but found no source / enricher work to do. Distinct
-- from `published` (real work landed) and `blocked` (gates failed
-- or Phase A didn't converge). Downstream consumers join on
-- `status='published'` and ignore no_op rows.
--
-- The actual CHECK constraint may have either of two names depending
-- on schema age (`graph_generations_status_check` is the inline-CREATE
-- name; `graph_generations_status_chk` is the name applied by some
-- mid-life migrations). Drop both, recreate with the canonical name.
ALTER TABLE okg.graph_generations
  DROP CONSTRAINT IF EXISTS graph_generations_status_check;
ALTER TABLE okg.graph_generations
  DROP CONSTRAINT IF EXISTS graph_generations_status_chk;
ALTER TABLE okg.graph_generations
  ADD CONSTRAINT graph_generations_status_chk
    CHECK (status IN (
      'pending','published','blocked','no_op','worker_write'
    ));

-- ========== Poros exchange outbox ==========
--
-- Sender-side durable evidence for graph.submit messages. The
-- outbox row is intentionally on the sender DB: it proves which
-- published generation a signed message was exported from before any
-- receiver admission decision exists.

CREATE TABLE IF NOT EXISTS okg.poros_outbox_messages (
  message_id              TEXT PRIMARY KEY,
  sender_authority_id     TEXT NOT NULL,
  recipient_authority_id  TEXT NOT NULL,
  sender_generation_id    TEXT NOT NULL,
  graph_id                TEXT NOT NULL,
  signing_key_id          TEXT NOT NULL,
  body_sha256             TEXT NOT NULL,
  envelope_sha256         TEXT NOT NULL,
  payload_mode            TEXT NOT NULL DEFAULT 'inline'
    CHECK (payload_mode IN ('inline', 'detached')),
  classification          TEXT NOT NULL,
  graph_profile           TEXT,
  root_node_ids           TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
  node_count              BIGINT NOT NULL DEFAULT 0
    CHECK (node_count >= 0),
  edge_count              BIGINT NOT NULL DEFAULT 0
    CHECK (edge_count >= 0),
  envelope                JSONB NOT NULL,
  body                    JSONB NOT NULL,
  status                  TEXT NOT NULL DEFAULT 'created'
    CHECK (status IN (
      'created', 'delivering', 'delivered', 'acknowledged', 'failed',
      'retrying', 'terminal_failed'
    )),
  max_delivery_attempts   BIGINT NOT NULL DEFAULT 3
    CHECK (max_delivery_attempts > 0),
  next_retry_at           TIMESTAMPTZ,
  last_error              TEXT,
  terminal_failure_reason TEXT,
  created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
  expires_at              TIMESTAMPTZ NOT NULL,
  updated_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE okg.poros_outbox_messages
  ADD COLUMN IF NOT EXISTS max_delivery_attempts BIGINT NOT NULL DEFAULT 3,
  ADD COLUMN IF NOT EXISTS next_retry_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS last_error TEXT,
  ADD COLUMN IF NOT EXISTS terminal_failure_reason TEXT;
ALTER TABLE okg.poros_outbox_messages
  DROP CONSTRAINT IF EXISTS poros_outbox_messages_status_check;
ALTER TABLE okg.poros_outbox_messages
  DROP CONSTRAINT IF EXISTS poros_outbox_messages_status_chk;
ALTER TABLE okg.poros_outbox_messages
  ADD CONSTRAINT poros_outbox_messages_status_chk
    CHECK (status IN (
      'created', 'delivering', 'delivered', 'acknowledged', 'failed',
      'retrying', 'terminal_failed'
    ));
ALTER TABLE okg.poros_outbox_messages
  DROP CONSTRAINT IF EXISTS poros_outbox_max_delivery_attempts_chk;
ALTER TABLE okg.poros_outbox_messages
  ADD CONSTRAINT poros_outbox_max_delivery_attempts_chk
    CHECK (max_delivery_attempts > 0);

CREATE INDEX IF NOT EXISTS poros_outbox_sender_generation_idx
  ON okg.poros_outbox_messages (
    sender_authority_id, sender_generation_id, created_at DESC
  );
CREATE INDEX IF NOT EXISTS poros_outbox_recipient_status_idx
  ON okg.poros_outbox_messages (
    recipient_authority_id, status, created_at DESC
  );
CREATE INDEX IF NOT EXISTS poros_outbox_graph_idx
  ON okg.poros_outbox_messages (graph_id, created_at DESC);
CREATE INDEX IF NOT EXISTS poros_outbox_body_hash_idx
  ON okg.poros_outbox_messages (body_sha256);
CREATE INDEX IF NOT EXISTS poros_outbox_retry_idx
  ON okg.poros_outbox_messages (status, next_retry_at, updated_at);

CREATE TABLE IF NOT EXISTS okg.poros_delivery_attempts (
  attempt_id              BIGSERIAL PRIMARY KEY,
  message_id              TEXT NOT NULL
    REFERENCES okg.poros_outbox_messages(message_id) ON DELETE CASCADE,
  sender_authority_id     TEXT NOT NULL,
  recipient_authority_id  TEXT NOT NULL,
  body_sha256             TEXT NOT NULL,
  envelope_sha256         TEXT NOT NULL,
  transport               TEXT NOT NULL DEFAULT 'local_cli',
  status                  TEXT NOT NULL
    CHECK (status IN (
      'attempted', 'accepted_for_admission', 'rejected_preflight',
      'duplicate', 'failed'
    )),
  reason_code             TEXT,
  receipt_sha256          TEXT,
  response                JSONB NOT NULL DEFAULT '{}'::jsonb,
  attempted_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
  completed_at            TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS poros_delivery_attempts_message_idx
  ON okg.poros_delivery_attempts (message_id, attempted_at DESC);

CREATE TABLE IF NOT EXISTS okg.federation_discovered_candidates (
  deployment_name          TEXT NOT NULL DEFAULT 'default',
  local_authority_id       TEXT NOT NULL,
  remote_authority_id      TEXT NOT NULL,
  discovery_hash           TEXT NOT NULL,
  document                 JSONB NOT NULL,
  status                   TEXT NOT NULL DEFAULT 'pending'
    CHECK (status IN ('pending', 'active', 'disabled', 'revoked')),
  created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (deployment_name, local_authority_id, remote_authority_id)
);

ALTER TABLE okg.federation_discovered_candidates
  ADD COLUMN IF NOT EXISTS deployment_name TEXT NOT NULL DEFAULT 'default';
ALTER TABLE okg.federation_discovered_candidates
  DROP CONSTRAINT IF EXISTS federation_discovered_candidates_pkey;
ALTER TABLE okg.federation_discovered_candidates
  ADD CONSTRAINT federation_discovered_candidates_pkey
  PRIMARY KEY (deployment_name, local_authority_id, remote_authority_id);

DROP INDEX IF EXISTS federation_candidates_status_idx;
CREATE INDEX IF NOT EXISTS federation_candidates_status_idx
  ON okg.federation_discovered_candidates (
    deployment_name, local_authority_id, status, updated_at DESC
  );

CREATE TABLE IF NOT EXISTS okg.federation_connection_requests (
  deployment_name          TEXT NOT NULL DEFAULT 'default',
  request_id               TEXT NOT NULL,
  local_authority_id       TEXT NOT NULL,
  requester_authority_id   TEXT NOT NULL,
  target_authority_id      TEXT NOT NULL,
  requested_action         TEXT NOT NULL,
  target_address           TEXT NOT NULL,
  requested_visibility     TEXT NOT NULL DEFAULT 'public',
  key_fingerprint          TEXT NOT NULL,
  discovery_hash           TEXT,
  status                   TEXT NOT NULL DEFAULT 'pending'
    CHECK (status IN ('pending', 'approved', 'denied', 'quarantined')),
  reviewer_identity        TEXT,
  nomos_decision_id        TEXT,
  nomos_decision_deployment_name TEXT NOT NULL DEFAULT 'default',
  decision_evidence        JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT federation_connection_requests_decision_scope_chk
    CHECK (
      nomos_decision_id IS NULL
      OR nomos_decision_deployment_name = deployment_name
    ),
  PRIMARY KEY (deployment_name, request_id)
);

ALTER TABLE okg.federation_connection_requests
  ADD COLUMN IF NOT EXISTS deployment_name TEXT NOT NULL DEFAULT 'default';
ALTER TABLE okg.federation_connection_requests
  DROP CONSTRAINT IF EXISTS federation_connection_requests_pkey;
ALTER TABLE okg.federation_connection_requests
  ADD CONSTRAINT federation_connection_requests_pkey
  PRIMARY KEY (deployment_name, request_id);
ALTER TABLE okg.federation_connection_requests
  DROP CONSTRAINT IF EXISTS federation_connection_requests_decision_scope_chk;
ALTER TABLE okg.federation_connection_requests
  ADD CONSTRAINT federation_connection_requests_decision_scope_chk
  CHECK (
    nomos_decision_id IS NULL
    OR nomos_decision_deployment_name = deployment_name
  );

DROP INDEX IF EXISTS federation_connection_requests_local_idx;
CREATE INDEX IF NOT EXISTS federation_connection_requests_local_idx
  ON okg.federation_connection_requests (
    deployment_name, local_authority_id, status, created_at DESC
  );
DROP INDEX IF EXISTS federation_connection_requests_target_idx;
CREATE INDEX IF NOT EXISTS federation_connection_requests_target_idx
  ON okg.federation_connection_requests (
    deployment_name, target_authority_id, target_address, status
  );

CREATE TABLE IF NOT EXISTS okg.federation_peer_trust_pins (
  deployment_name          TEXT NOT NULL DEFAULT 'default',
  local_authority_id       TEXT NOT NULL,
  remote_authority_id      TEXT NOT NULL,
  key_fingerprint          TEXT NOT NULL,
  allowed_actions          TEXT[] NOT NULL DEFAULT '{}',
  allowed_boards           TEXT[] NOT NULL DEFAULT '{}',
  allowed_pull_surfaces    TEXT[] NOT NULL DEFAULT '{}',
  visibility_tiers         TEXT[] NOT NULL DEFAULT '{public}',
  status                   TEXT NOT NULL DEFAULT 'active'
    CHECK (status IN ('active', 'disabled', 'revoked')),
  nomos_decision_id        TEXT,
  nomos_decision_deployment_name TEXT NOT NULL DEFAULT 'default',
  nomos_effect             TEXT NOT NULL DEFAULT 'allow'
    CHECK (nomos_effect IN ('allow', 'deny', 'review', 'quarantine')),
  decision_evidence        JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT federation_peer_trust_pins_decision_scope_chk
    CHECK (
      nomos_decision_id IS NULL
      OR nomos_decision_deployment_name = deployment_name
    ),
  PRIMARY KEY (
    deployment_name,
    local_authority_id,
    remote_authority_id,
    key_fingerprint
  )
);

ALTER TABLE okg.federation_peer_trust_pins
  ADD COLUMN IF NOT EXISTS deployment_name TEXT NOT NULL DEFAULT 'default';
ALTER TABLE okg.federation_peer_trust_pins
  DROP CONSTRAINT IF EXISTS federation_peer_trust_pins_pkey;
ALTER TABLE okg.federation_peer_trust_pins
  ADD CONSTRAINT federation_peer_trust_pins_pkey
  PRIMARY KEY (
    deployment_name,
    local_authority_id,
    remote_authority_id,
    key_fingerprint
  );
ALTER TABLE okg.federation_peer_trust_pins
  DROP CONSTRAINT IF EXISTS federation_peer_trust_pins_decision_scope_chk;
ALTER TABLE okg.federation_peer_trust_pins
  ADD CONSTRAINT federation_peer_trust_pins_decision_scope_chk
  CHECK (
    nomos_decision_id IS NULL
    OR nomos_decision_deployment_name = deployment_name
  );

DROP INDEX IF EXISTS federation_peer_trust_pins_status_idx;
CREATE INDEX IF NOT EXISTS federation_peer_trust_pins_status_idx
  ON okg.federation_peer_trust_pins (
    deployment_name, local_authority_id, remote_authority_id, status, updated_at DESC
  );

CREATE TABLE IF NOT EXISTS okg.federation_route_events (
  deployment_name          TEXT NOT NULL DEFAULT 'default',
  route_event_id           TEXT NOT NULL,
  local_authority_id       TEXT NOT NULL,
  remote_authority_id      TEXT NOT NULL,
  subject                  TEXT NOT NULL,
  action                   TEXT NOT NULL,
  target_address           TEXT NOT NULL,
  visibility               TEXT NOT NULL DEFAULT 'public',
  decision                 TEXT NOT NULL CHECK (decision IN ('allow', 'deny')),
  reason_code              TEXT NOT NULL,
  nomos_decision_id        TEXT,
  nomos_decision_deployment_name TEXT NOT NULL DEFAULT 'default',
  trust_pin_fingerprint    TEXT,
  live_row_delta           JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT federation_route_events_decision_scope_chk
    CHECK (
      nomos_decision_id IS NULL
      OR nomos_decision_deployment_name = deployment_name
    ),
  PRIMARY KEY (deployment_name, route_event_id)
);

ALTER TABLE okg.federation_route_events
  ADD COLUMN IF NOT EXISTS deployment_name TEXT NOT NULL DEFAULT 'default';
ALTER TABLE okg.federation_route_events
  DROP CONSTRAINT IF EXISTS federation_route_events_pkey;
ALTER TABLE okg.federation_route_events
  ADD CONSTRAINT federation_route_events_pkey
  PRIMARY KEY (deployment_name, route_event_id);
ALTER TABLE okg.federation_route_events
  DROP CONSTRAINT IF EXISTS federation_route_events_decision_scope_chk;
ALTER TABLE okg.federation_route_events
  ADD CONSTRAINT federation_route_events_decision_scope_chk
  CHECK (
    nomos_decision_id IS NULL
    OR nomos_decision_deployment_name = deployment_name
  );

DROP INDEX IF EXISTS federation_route_events_local_idx;
CREATE INDEX IF NOT EXISTS federation_route_events_local_idx
  ON okg.federation_route_events (
    deployment_name, local_authority_id, remote_authority_id, created_at DESC
  );
DROP INDEX IF EXISTS federation_route_events_decision_idx;
CREATE INDEX IF NOT EXISTS federation_route_events_decision_idx
  ON okg.federation_route_events (
    deployment_name, decision, reason_code, created_at DESC
  );

CREATE TABLE IF NOT EXISTS okg.federation_mailbox_messages (
  message_id              TEXT PRIMARY KEY,
  sender_authority_id     TEXT NOT NULL,
  recipient_authority_id  TEXT NOT NULL,
  payload_mode            TEXT NOT NULL
    CHECK (payload_mode IN ('inline', 'detached_pointer')),
  packet_sha256           TEXT NOT NULL,
  body_sha256             TEXT,
  envelope                JSONB,
  pointer                 JSONB,
  object_uri              TEXT,
  object_sha256           TEXT,
  size_bytes              BIGINT CHECK (size_bytes IS NULL OR size_bytes >= 0),
  expires_at              TIMESTAMPTZ NOT NULL,
  status                  TEXT NOT NULL DEFAULT 'pending'
    CHECK (status IN ('pending', 'leased', 'acknowledged', 'denied')),
  lease_token             TEXT,
  lease_expires_at        TIMESTAMPTZ,
  receipt_evidence        JSONB,
  denial_evidence         JSONB,
  deny_reason_code        TEXT,
  created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE okg.federation_mailbox_messages
  DROP CONSTRAINT IF EXISTS federation_mailbox_payload_shape_chk;
ALTER TABLE okg.federation_mailbox_messages
  ADD CONSTRAINT federation_mailbox_payload_shape_chk
    CHECK (
      (
        payload_mode = 'inline'
        AND envelope IS NOT NULL
        AND pointer IS NULL
        AND object_uri IS NULL
        AND object_sha256 IS NULL
        AND size_bytes IS NULL
      )
      OR
      (
        payload_mode = 'detached_pointer'
        AND envelope IS NULL
        AND pointer IS NOT NULL
        AND NULLIF(btrim(object_uri), '') IS NOT NULL
        AND NULLIF(btrim(object_sha256), '') IS NOT NULL
        AND size_bytes IS NOT NULL
        AND size_bytes > 0
      )
    );
ALTER TABLE okg.federation_mailbox_messages
  DROP CONSTRAINT IF EXISTS federation_mailbox_lifecycle_evidence_chk;
ALTER TABLE okg.federation_mailbox_messages
  ADD CONSTRAINT federation_mailbox_lifecycle_evidence_chk
    CHECK (
      (
        status = 'pending'
        AND receipt_evidence IS NULL
        AND denial_evidence IS NULL
        AND deny_reason_code IS NULL
      )
      OR
      (
        status = 'leased'
        AND lease_token IS NOT NULL
        AND lease_expires_at IS NOT NULL
        AND receipt_evidence IS NULL
        AND denial_evidence IS NULL
        AND deny_reason_code IS NULL
      )
      OR
      (
        status = 'acknowledged'
        AND receipt_evidence IS NOT NULL
        AND denial_evidence IS NULL
        AND deny_reason_code IS NULL
      )
      OR
      (
        status = 'denied'
        AND receipt_evidence IS NULL
        AND denial_evidence IS NOT NULL
        AND NULLIF(btrim(deny_reason_code), '') IS NOT NULL
      )
    );

CREATE INDEX IF NOT EXISTS federation_mailbox_recipient_status_idx
  ON okg.federation_mailbox_messages (
    recipient_authority_id, status, expires_at, created_at
  );
CREATE INDEX IF NOT EXISTS federation_mailbox_sender_idx
  ON okg.federation_mailbox_messages (
    sender_authority_id, created_at DESC
  );
CREATE INDEX IF NOT EXISTS federation_mailbox_packet_hash_idx
  ON okg.federation_mailbox_messages (packet_sha256);
CREATE INDEX IF NOT EXISTS federation_mailbox_lease_idx
  ON okg.federation_mailbox_messages (
    status, lease_expires_at
  ) WHERE status = 'leased';

DO $federation_mailbox_lease_shape$
BEGIN
  IF NOT EXISTS (
    SELECT 1
      FROM pg_constraint
     WHERE conrelid = 'okg.federation_mailbox_messages'::regclass
       AND conname = 'federation_mailbox_lease_shape_chk'
  ) THEN
    ALTER TABLE okg.federation_mailbox_messages
      ADD CONSTRAINT federation_mailbox_lease_shape_chk
      CHECK (
        status <> 'leased'
        OR (
          lease_token IS NOT NULL
          AND length(lease_token) > 0
          AND lease_expires_at IS NOT NULL
        )
      );
  END IF;
END
$federation_mailbox_lease_shape$;

CREATE TABLE IF NOT EXISTS okg.federation_mailbox_events (
  event_id                BIGSERIAL PRIMARY KEY,
  message_id              TEXT NOT NULL
    REFERENCES okg.federation_mailbox_messages(message_id) ON DELETE CASCADE,
  event_type              TEXT NOT NULL,
  sender_authority_id     TEXT NOT NULL,
  recipient_authority_id  TEXT NOT NULL,
  status                  TEXT NOT NULL,
  reason_code             TEXT,
  payload                 JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS federation_mailbox_events_message_idx
  ON okg.federation_mailbox_events (message_id, created_at DESC);
CREATE INDEX IF NOT EXISTS federation_mailbox_events_recipient_idx
  ON okg.federation_mailbox_events (
    recipient_authority_id, created_at DESC
  );

CREATE TABLE IF NOT EXISTS okg.poros_inbox_messages (
  inbox_id                BIGSERIAL PRIMARY KEY,
  receiver_authority_id   TEXT NOT NULL,
  sender_authority_id     TEXT NOT NULL,
  message_id              TEXT NOT NULL,
  status                  TEXT NOT NULL,
  reason_code             TEXT,
  body_sha256             TEXT,
  signing_key_id          TEXT,
  envelope_sha256         TEXT NOT NULL,
  receipt_sha256          TEXT,
  envelope                JSONB NOT NULL,
  receipt                 JSONB,
  admission_candidate     JSONB,
  trusted                 BOOLEAN NOT NULL DEFAULT false,
  signature_valid         BOOLEAN NOT NULL DEFAULT false,
  recipient_valid         BOOLEAN NOT NULL DEFAULT false,
  body_hash_valid         BOOLEAN NOT NULL DEFAULT false,
  fresh                   BOOLEAN NOT NULL DEFAULT false,
  received_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
  created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (receiver_authority_id, message_id)
);

ALTER TABLE okg.poros_inbox_messages
  DROP CONSTRAINT IF EXISTS poros_inbox_messages_status_chk;
ALTER TABLE okg.poros_inbox_messages
  ADD CONSTRAINT poros_inbox_messages_status_chk
    CHECK (status IN (
      'accepted_for_admission',
      'rejected_preflight',
      'duplicate',
      'quarantined'
    ));

CREATE INDEX IF NOT EXISTS poros_inbox_receiver_status_idx
  ON okg.poros_inbox_messages (
    receiver_authority_id, status, received_at DESC
  );
CREATE INDEX IF NOT EXISTS poros_inbox_sender_idx
  ON okg.poros_inbox_messages (
    sender_authority_id, received_at DESC
  );
CREATE INDEX IF NOT EXISTS poros_inbox_body_hash_idx
  ON okg.poros_inbox_messages (body_sha256)
  WHERE body_sha256 IS NOT NULL;

CREATE TABLE IF NOT EXISTS okg.poros_replay_cache (
  receiver_authority_id   TEXT NOT NULL,
  sender_authority_id     TEXT NOT NULL,
  message_id              TEXT NOT NULL,
  body_sha256             TEXT,
  status                  TEXT NOT NULL,
  reason_code             TEXT,
  receipt_sha256          TEXT,
  expires_at              TIMESTAMPTZ NOT NULL,
  first_seen_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
  duplicate_count         BIGINT NOT NULL DEFAULT 0
    CHECK (duplicate_count >= 0),
  PRIMARY KEY (
    receiver_authority_id, sender_authority_id, message_id
  )
);

CREATE INDEX IF NOT EXISTS poros_replay_cache_expires_idx
  ON okg.poros_replay_cache (expires_at);

CREATE TABLE IF NOT EXISTS okg.poros_detached_object_manifests (
  manifest_id             BIGSERIAL PRIMARY KEY,
  message_id              TEXT NOT NULL,
  body_sha256             TEXT NOT NULL,
  authority_id            TEXT NOT NULL,
  side                    TEXT NOT NULL CHECK (side IN ('sender','receiver')),
  object_id               TEXT NOT NULL,
  media_type              TEXT,
  size_bytes              BIGINT NOT NULL CHECK (size_bytes >= 0),
  object_sha256           TEXT NOT NULL,
  chunk_count             BIGINT NOT NULL DEFAULT 0
    CHECK (chunk_count >= 0),
  classification          TEXT NOT NULL DEFAULT 'public',
  encrypted               BOOLEAN NOT NULL DEFAULT false,
  manifest                JSONB NOT NULL,
  created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (side, authority_id, message_id, object_id)
);

ALTER TABLE okg.poros_detached_object_manifests
  DROP CONSTRAINT IF EXISTS poros_detached_manifest_classification_chk;
ALTER TABLE okg.poros_detached_object_manifests
  ADD CONSTRAINT poros_detached_manifest_classification_chk
    CHECK (classification IN (
      'public',
      'internal',
      'restricted',
      'private',
      'unspecified'
    ));
ALTER TABLE okg.poros_detached_object_manifests
  DROP CONSTRAINT IF EXISTS poros_detached_manifest_shape_chk;
ALTER TABLE okg.poros_detached_object_manifests
  ADD CONSTRAINT poros_detached_manifest_shape_chk
    CHECK (
      COALESCE(jsonb_typeof(manifest) = 'object', false)
      AND COALESCE(manifest->>'object_id' = object_id, false)
      AND (
        NOT (manifest ? 'classification')
        OR COALESCE(manifest->>'classification' = classification, false)
      )
      AND (
        (
          encrypted
          AND manifest ? 'ciphertext_sha256'
          AND COALESCE(jsonb_typeof(manifest->'encryption') = 'object', false)
          AND COALESCE(object_sha256 = manifest->>'ciphertext_sha256', false)
        )
        OR
        (
          NOT encrypted
          AND NOT (manifest ? 'ciphertext_sha256')
          AND NOT (manifest ? 'encryption')
          AND (
            COALESCE(object_sha256 = manifest->>'cleartext_sha256', false)
            OR COALESCE(object_sha256 = manifest->>'sha256', false)
          )
        )
      )
    );

CREATE INDEX IF NOT EXISTS poros_detached_manifest_message_idx
  ON okg.poros_detached_object_manifests (message_id, body_sha256);

CREATE OR REPLACE VIEW okg.v_exchange_boundary_violations AS
SELECT
  'federation_mailbox_messages'::TEXT AS relation_name,
  message_id AS record_id,
  'payload_shape'::TEXT AS violation_code
FROM okg.federation_mailbox_messages
WHERE NOT COALESCE(
  (
    (
      payload_mode = 'inline'
      AND envelope IS NOT NULL
      AND pointer IS NULL
      AND object_uri IS NULL
      AND object_sha256 IS NULL
      AND size_bytes IS NULL
    )
    OR
    (
      payload_mode = 'detached_pointer'
      AND envelope IS NULL
      AND pointer IS NOT NULL
      AND NULLIF(btrim(object_uri), '') IS NOT NULL
      AND NULLIF(btrim(object_sha256), '') IS NOT NULL
      AND size_bytes IS NOT NULL
      AND size_bytes > 0
    )
  ),
  false
)
UNION ALL
SELECT
  'federation_mailbox_messages'::TEXT,
  message_id,
  'lifecycle_evidence'::TEXT
FROM okg.federation_mailbox_messages
WHERE NOT COALESCE(
  (
    (
      status = 'pending'
      AND receipt_evidence IS NULL
      AND denial_evidence IS NULL
      AND deny_reason_code IS NULL
    )
    OR
    (
      status = 'leased'
      AND lease_token IS NOT NULL
      AND lease_expires_at IS NOT NULL
      AND receipt_evidence IS NULL
      AND denial_evidence IS NULL
      AND deny_reason_code IS NULL
    )
    OR
    (
      status = 'acknowledged'
      AND receipt_evidence IS NOT NULL
      AND denial_evidence IS NULL
      AND deny_reason_code IS NULL
    )
    OR
    (
      status = 'denied'
      AND receipt_evidence IS NULL
      AND denial_evidence IS NOT NULL
      AND NULLIF(btrim(deny_reason_code), '') IS NOT NULL
    )
  ),
  false
)
UNION ALL
SELECT
  'poros_inbox_messages'::TEXT,
  receiver_authority_id || ':' || message_id,
  'status_vocabulary'::TEXT
FROM okg.poros_inbox_messages
WHERE status NOT IN (
  'accepted_for_admission',
  'rejected_preflight',
  'duplicate',
  'quarantined'
)
UNION ALL
SELECT
  'poros_detached_object_manifests'::TEXT,
  side || ':' || authority_id || ':' || message_id || ':' || object_id,
  'classification'::TEXT
FROM okg.poros_detached_object_manifests
WHERE classification NOT IN (
  'public',
  'internal',
  'restricted',
  'private',
  'unspecified'
)
UNION ALL
SELECT
  'poros_detached_object_manifests'::TEXT,
  side || ':' || authority_id || ':' || message_id || ':' || object_id,
  'manifest_shape'::TEXT
FROM okg.poros_detached_object_manifests
WHERE NOT COALESCE(
  (
    COALESCE(jsonb_typeof(manifest) = 'object', false)
    AND COALESCE(manifest->>'object_id' = object_id, false)
    AND (
      NOT (manifest ? 'classification')
      OR COALESCE(manifest->>'classification' = classification, false)
    )
    AND (
      (
        encrypted
        AND manifest ? 'ciphertext_sha256'
        AND COALESCE(jsonb_typeof(manifest->'encryption') = 'object', false)
        AND COALESCE(object_sha256 = manifest->>'ciphertext_sha256', false)
      )
      OR
      (
        NOT encrypted
        AND NOT (manifest ? 'ciphertext_sha256')
        AND NOT (manifest ? 'encryption')
        AND (
          COALESCE(object_sha256 = manifest->>'cleartext_sha256', false)
          OR COALESCE(object_sha256 = manifest->>'sha256', false)
        )
      )
    )
  ),
  false
);

CREATE TABLE IF NOT EXISTS okg.poros_incorporation_events (
  event_id                BIGSERIAL PRIMARY KEY,
  receiver_authority_id   TEXT NOT NULL,
  sender_authority_id     TEXT NOT NULL,
  message_id              TEXT NOT NULL,
  body_sha256             TEXT,
  event_type              TEXT NOT NULL
    CHECK (event_type IN (
      'preflight_accepted', 'preflight_rejected', 'duplicate',
      'nomos_decided', 'admitted', 'published', 'blocked',
      'quarantined', 'review', 'promotion_decided',
      'promotion_denied', 'promoted', 'rejection_decided',
      'rejection_denied', 'rejected'
    )),
  status                  TEXT NOT NULL,
  reason_code             TEXT,
  nomos_decision_id       TEXT,
  nomos_decision_deployment_name TEXT NOT NULL DEFAULT 'default',
  policy_bundle_id        TEXT,
  policy_fingerprint      TEXT,
  nomos_input_hash        TEXT,
  admission_id            UUID
    REFERENCES okg.admission_envelopes(admission_id) ON DELETE SET NULL,
  receiver_generation_id  TEXT,
  payload                 JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS poros_incorporation_message_idx
  ON okg.poros_incorporation_events (message_id, created_at DESC);
CREATE INDEX IF NOT EXISTS poros_incorporation_decision_idx
  ON okg.poros_incorporation_events (nomos_decision_id)
  WHERE nomos_decision_id IS NOT NULL;

ALTER TABLE okg.poros_incorporation_events
  ADD COLUMN IF NOT EXISTS policy_fingerprint TEXT;

ALTER TABLE okg.poros_incorporation_events
  DROP CONSTRAINT IF EXISTS poros_incorporation_events_event_type_check;
ALTER TABLE okg.poros_incorporation_events
  ADD CONSTRAINT poros_incorporation_events_event_type_check
  CHECK (event_type IN (
    'preflight_accepted', 'preflight_rejected', 'duplicate',
    'nomos_decided', 'admitted', 'published', 'blocked',
    'quarantined', 'review', 'promotion_decided',
    'promotion_denied', 'promoted', 'rejection_decided',
    'rejection_denied', 'rejected'
  ));

-- ========== Nomos active policy and configuration ==========
--
-- Nomos policy/config rows are the Postgres-backed active control
-- plane. Runtime code reads active activations and fails closed when
-- the expected scope has no active bundle or config set.

CREATE TABLE IF NOT EXISTS okg.nomos_policy_bundles (
  bundle_row_id        BIGSERIAL PRIMARY KEY,
  deployment_name      TEXT NOT NULL,
  policy_bundle_id     TEXT NOT NULL,
  policy_version       TEXT NOT NULL,
  policy_fingerprint   TEXT NOT NULL,
  compiler_version     TEXT NOT NULL,
  source_generation_id TEXT COLLATE "C",
  scope                JSONB NOT NULL DEFAULT '{}'::jsonb,
  default_effect       TEXT NOT NULL
    CHECK (default_effect IN (
      'allow', 'deny', 'review', 'quarantine', 'redact', 'declassify'
    )),
  reason_code_catalog  JSONB NOT NULL DEFAULT '{}'::jsonb,
  bundle               JSONB NOT NULL,
  status               TEXT NOT NULL DEFAULT 'compiled'
    CHECK (status IN ('compiled', 'active', 'disabled', 'superseded')),
  effective_from       TIMESTAMPTZ,
  effective_until      TIMESTAMPTZ,
  compiled_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  metadata             JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (deployment_name, policy_fingerprint)
);

CREATE INDEX IF NOT EXISTS nomos_policy_bundles_deployment_idx
  ON okg.nomos_policy_bundles (
    deployment_name, policy_bundle_id, status
  );

CREATE TABLE IF NOT EXISTS okg.nomos_policy_activations (
  activation_row_id     BIGSERIAL PRIMARY KEY,
  deployment_name       TEXT NOT NULL,
  activation_id         TEXT NOT NULL,
  scope                 TEXT NOT NULL,
  policy_fingerprint    TEXT NOT NULL,
  status                TEXT NOT NULL DEFAULT 'active'
    CHECK (status IN ('active', 'superseded', 'disabled')),
  activated_by          TEXT NOT NULL,
  activation_decision_id TEXT NOT NULL,
  metadata              JSONB NOT NULL DEFAULT '{}'::jsonb,
  activated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  superseded_at         TIMESTAMPTZ,
  UNIQUE (deployment_name, activation_id),
  FOREIGN KEY (deployment_name, policy_fingerprint)
    REFERENCES okg.nomos_policy_bundles (
      deployment_name, policy_fingerprint
    ) ON DELETE RESTRICT
);

CREATE UNIQUE INDEX IF NOT EXISTS nomos_policy_active_scope_idx
  ON okg.nomos_policy_activations (deployment_name, scope)
  WHERE status = 'active';

CREATE TABLE IF NOT EXISTS okg.nomos_config_sets (
  config_row_id        BIGSERIAL PRIMARY KEY,
  deployment_name      TEXT NOT NULL,
  config_set_id        TEXT NOT NULL,
  config_version       TEXT NOT NULL,
  config_fingerprint   TEXT NOT NULL,
  compiler_version     TEXT NOT NULL,
  source_generation_id TEXT COLLATE "C",
  scope                JSONB NOT NULL DEFAULT '{}'::jsonb,
  entries              JSONB NOT NULL DEFAULT '[]'::jsonb,
  config_set           JSONB NOT NULL,
  status               TEXT NOT NULL DEFAULT 'compiled'
    CHECK (status IN ('compiled', 'active', 'disabled', 'superseded')),
  effective_from       TIMESTAMPTZ,
  effective_until      TIMESTAMPTZ,
  compiled_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  metadata             JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (deployment_name, config_fingerprint)
);

CREATE INDEX IF NOT EXISTS nomos_config_sets_deployment_idx
  ON okg.nomos_config_sets (
    deployment_name, config_set_id, status
  );

CREATE TABLE IF NOT EXISTS okg.nomos_config_activations (
  activation_row_id      BIGSERIAL PRIMARY KEY,
  deployment_name        TEXT NOT NULL,
  activation_id          TEXT NOT NULL,
  scope                  TEXT NOT NULL,
  config_fingerprint     TEXT NOT NULL,
  status                 TEXT NOT NULL DEFAULT 'active'
    CHECK (status IN ('active', 'superseded', 'disabled')),
  activated_by           TEXT NOT NULL,
  activation_decision_id TEXT NOT NULL,
  metadata               JSONB NOT NULL DEFAULT '{}'::jsonb,
  activated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  superseded_at          TIMESTAMPTZ,
  UNIQUE (deployment_name, activation_id),
  FOREIGN KEY (deployment_name, config_fingerprint)
    REFERENCES okg.nomos_config_sets (
      deployment_name, config_fingerprint
    ) ON DELETE RESTRICT
);

CREATE UNIQUE INDEX IF NOT EXISTS nomos_config_active_scope_idx
  ON okg.nomos_config_activations (deployment_name, scope)
  WHERE status = 'active';

-- ========== Nomos identity provider evidence ==========
--
-- Nomos consumes verified institutional identity evidence from SSO,
-- directory sync, service accounts, signed Poros authorities, and
-- deployment-local agent identities. These tables store provider metadata
-- and redacted/fingerprinted claim evidence only; raw tokens, SAML
-- assertions, passwords, cookies, and private keys stay outside AKMON.

CREATE TABLE IF NOT EXISTS okg.nomos_identity_providers (
  provider_row_id       BIGSERIAL PRIMARY KEY,
  deployment_name       TEXT NOT NULL,
  provider_id           TEXT NOT NULL,
  provider_type         TEXT NOT NULL
    CHECK (provider_type IN (
      'agent_local', 'deployment_extension', 'kerberos_gssapi',
      'ldap', 'oidc', 'poros_authority', 'saml', 'scim',
      'service_account', 'touchstone_oidc'
    )),
  issuer                TEXT NOT NULL,
  audiences             TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
  subject_claim         TEXT NOT NULL DEFAULT 'sub',
  group_claims          TEXT[] NOT NULL DEFAULT ARRAY['groups']::TEXT[],
  role_claims           TEXT[] NOT NULL DEFAULT ARRAY['roles']::TEXT[],
  entitlement_claims    TEXT[] NOT NULL DEFAULT ARRAY['entitlements']::TEXT[],
  assurance_claim       TEXT,
  key_reference         TEXT,
  secret_references     TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
  claim_allowlist       TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
  required_assurance    TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
  mfa_required          BOOLEAN NOT NULL DEFAULT FALSE,
  group_claims_approved BOOLEAN NOT NULL DEFAULT FALSE,
  metadata_fingerprint  TEXT,
  realm                 TEXT,
  service_principal     TEXT,
  freshness_seconds     BIGINT CHECK (
    freshness_seconds IS NULL OR freshness_seconds > 0
  ),
  status                TEXT NOT NULL DEFAULT 'active'
    CHECK (status IN ('active', 'disabled', 'rotating', 'revoked')),
  metadata              JSONB NOT NULL DEFAULT '{}'::jsonb,
  effective_from        TIMESTAMPTZ,
  effective_until       TIMESTAMPTZ,
  created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (deployment_name, provider_id)
);

CREATE INDEX IF NOT EXISTS nomos_identity_providers_deployment_idx
  ON okg.nomos_identity_providers (
    deployment_name, provider_id, status
  );

ALTER TABLE okg.nomos_identity_providers
  ADD COLUMN IF NOT EXISTS secret_references TEXT[] NOT NULL
    DEFAULT ARRAY[]::TEXT[],
  ADD COLUMN IF NOT EXISTS required_assurance TEXT[] NOT NULL
    DEFAULT ARRAY[]::TEXT[],
  ADD COLUMN IF NOT EXISTS mfa_required BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS group_claims_approved BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS metadata_fingerprint TEXT,
  ADD COLUMN IF NOT EXISTS realm TEXT,
  ADD COLUMN IF NOT EXISTS service_principal TEXT;

DO $$
BEGIN
  ALTER TABLE okg.nomos_identity_providers
    DROP CONSTRAINT IF EXISTS nomos_identity_providers_provider_type_check;
  ALTER TABLE okg.nomos_identity_providers
    ADD CONSTRAINT nomos_identity_providers_provider_type_check
    -- Keep in lockstep with IDENTITY_PROVIDER_TYPES
    -- (src/okg/substrate/nomos/identity.py). `mcp_principal_header` is PACT
    -- mcp-principal-mapping: a principal asserted in a declared request
    -- header by a transport-authenticated fronting application. The
    -- DROP/ADD above makes this block re-runnable, so an existing database
    -- picks the new value up on the next schema apply.
    CHECK (provider_type IN (
      'agent_local', 'deployment_extension', 'kerberos_gssapi',
      'ldap', 'mcp_principal_header', 'oidc', 'poros_authority', 'saml',
      'scim', 'service_account', 'touchstone_oidc'
    ));
  ALTER TABLE okg.nomos_identity_providers
    DROP CONSTRAINT IF EXISTS nomos_identity_providers_status_check;
  ALTER TABLE okg.nomos_identity_providers
    ADD CONSTRAINT nomos_identity_providers_status_check
    CHECK (status IN ('active', 'disabled', 'rotating', 'revoked'));
END $$;

CREATE TABLE IF NOT EXISTS okg.nomos_external_claims (
  claim_row_id          BIGSERIAL PRIMARY KEY,
  deployment_name       TEXT NOT NULL,
  provider_id           TEXT NOT NULL,
  provider_type         TEXT NOT NULL,
  issuer                TEXT NOT NULL,
  external_subject      TEXT NOT NULL,
  audiences             TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
  groups                TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
  roles                 TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
  entitlements          TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
  assurance             TEXT,
  assurance_values      TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
  mfa_satisfied         BOOLEAN NOT NULL DEFAULT FALSE,
  realm                 TEXT,
  service_principal     TEXT,
  delegation_chain      TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
  expires_at            TIMESTAMPTZ,
  not_before            TIMESTAMPTZ,
  auth_time             TIMESTAMPTZ,
  claim_fingerprint     TEXT NOT NULL,
  verification_status   TEXT NOT NULL,
  verification_error    TEXT,
  evidence              JSONB NOT NULL DEFAULT '{}'::jsonb,
  nomos_decision_id     TEXT,
  observed_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (deployment_name, provider_id, claim_fingerprint)
);

CREATE INDEX IF NOT EXISTS nomos_external_claims_provider_idx
  ON okg.nomos_external_claims (
    deployment_name, provider_id, observed_at DESC
  );
CREATE INDEX IF NOT EXISTS nomos_external_claims_fingerprint_idx
  ON okg.nomos_external_claims (claim_fingerprint);
CREATE INDEX IF NOT EXISTS nomos_external_claims_decision_idx
  ON okg.nomos_external_claims (nomos_decision_id)
  WHERE nomos_decision_id IS NOT NULL;

ALTER TABLE okg.nomos_external_claims
  ADD COLUMN IF NOT EXISTS assurance_values TEXT[] NOT NULL
    DEFAULT ARRAY[]::TEXT[],
  ADD COLUMN IF NOT EXISTS mfa_satisfied BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS realm TEXT,
  ADD COLUMN IF NOT EXISTS service_principal TEXT,
  ADD COLUMN IF NOT EXISTS delegation_chain TEXT[] NOT NULL
    DEFAULT ARRAY[]::TEXT[],
  ADD COLUMN IF NOT EXISTS verification_error TEXT;

CREATE TABLE IF NOT EXISTS okg.nomos_subject_bindings (
  binding_row_id       BIGSERIAL PRIMARY KEY,
  deployment_name      TEXT NOT NULL,
  binding_id           TEXT NOT NULL,
  provider_id          TEXT NOT NULL,
  issuer               TEXT NOT NULL,
  claim_path           TEXT NOT NULL,
  claim_value          TEXT NOT NULL,
  subject              TEXT NOT NULL,
  scope                TEXT NOT NULL,
  priority             BIGINT NOT NULL DEFAULT 0,
  required_assurance   TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
  status               TEXT NOT NULL DEFAULT 'active'
    CHECK (status IN ('active', 'disabled')),
  effective_from       TIMESTAMPTZ,
  effective_until      TIMESTAMPTZ,
  metadata             JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (deployment_name, binding_id)
);

CREATE INDEX IF NOT EXISTS nomos_subject_bindings_provider_idx
  ON okg.nomos_subject_bindings (
    deployment_name, provider_id, issuer, scope, status
  );
CREATE INDEX IF NOT EXISTS nomos_subject_bindings_subject_idx
  ON okg.nomos_subject_bindings (
    deployment_name, subject, scope, status
  );

ALTER TABLE okg.nomos_subject_bindings
  ADD COLUMN IF NOT EXISTS required_assurance TEXT[] NOT NULL
    DEFAULT ARRAY[]::TEXT[];

CREATE TABLE IF NOT EXISTS okg.nomos_permission_grants (
  grant_row_id         BIGSERIAL PRIMARY KEY,
  deployment_name      TEXT NOT NULL,
  grant_id             TEXT NOT NULL,
  subject              TEXT NOT NULL,
  actions              TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
  scope                TEXT NOT NULL,
  effect               TEXT NOT NULL DEFAULT 'allow'
    CHECK (effect IN (
      'allow', 'deny', 'review', 'quarantine', 'redact', 'declassify'
    )),
  resource_match       JSONB NOT NULL DEFAULT '{}'::jsonb,
  classifications      TEXT[] NOT NULL DEFAULT ARRAY['*']::TEXT[],
  priority             BIGINT NOT NULL DEFAULT 0,
  governing_rule_ids   TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
  status               TEXT NOT NULL DEFAULT 'active'
    CHECK (status IN ('active', 'disabled')),
  metadata             JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (deployment_name, grant_id)
);

CREATE INDEX IF NOT EXISTS nomos_permission_grants_subject_idx
  ON okg.nomos_permission_grants (
    deployment_name, subject, scope, status
  );

CREATE TABLE IF NOT EXISTS okg.nomos_decisions (
  decision_row_id      BIGSERIAL PRIMARY KEY,
  deployment_name      TEXT NOT NULL,
  decision_id          TEXT NOT NULL,
  policy_bundle_id     TEXT,
  policy_version       TEXT,
  policy_fingerprint   TEXT,
  input_hash           TEXT NOT NULL,
  subject              TEXT NOT NULL,
  action               TEXT NOT NULL,
  resource             JSONB NOT NULL DEFAULT '{}'::jsonb,
  classification       TEXT NOT NULL,
  context              JSONB NOT NULL DEFAULT '{}'::jsonb,
  effect               TEXT NOT NULL
    CHECK (effect IN (
      'allow', 'deny', 'review', 'quarantine', 'redact', 'declassify'
    )),
  reason_code          TEXT NOT NULL,
  matched_rule_ids     TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
  obligations          JSONB NOT NULL DEFAULT '[]'::jsonb,
  enforcement_point    TEXT NOT NULL,
  target               JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (deployment_name, decision_id)
);

CREATE INDEX IF NOT EXISTS nomos_decisions_deployment_created_idx
  ON okg.nomos_decisions (deployment_name, created_at DESC);
CREATE INDEX IF NOT EXISTS nomos_decisions_effect_idx
  ON okg.nomos_decisions (deployment_name, effect, created_at DESC);
CREATE INDEX IF NOT EXISTS nomos_decisions_input_hash_idx
  ON okg.nomos_decisions (input_hash);

-- Nomos decision references moved from doctor-only detection to schema
-- enforcement. References remain deployment-scoped to match the canonical
-- okg.nomos_decisions(deployment_name, decision_id) key: decision_id values are
-- content-derived and may repeat across deployments.
ALTER TABLE okg.federation_connection_requests
  ADD COLUMN IF NOT EXISTS nomos_decision_deployment_name TEXT NOT NULL
    DEFAULT 'default';
ALTER TABLE okg.federation_peer_trust_pins
  ADD COLUMN IF NOT EXISTS nomos_decision_deployment_name TEXT NOT NULL
    DEFAULT 'default';
ALTER TABLE okg.federation_route_events
  ADD COLUMN IF NOT EXISTS nomos_decision_deployment_name TEXT NOT NULL
    DEFAULT 'default';
ALTER TABLE okg.poros_incorporation_events
  ADD COLUMN IF NOT EXISTS nomos_decision_deployment_name TEXT NOT NULL
    DEFAULT 'default';

-- Drop only the legacy PARENT-side single-column key (cheap catalog op, no
-- table scan). The child composite FK drop/re-add is NOT done here: it is
-- guarded per-child inside the DO block below so a converged deployment never
-- drops and re-validates an FK it already has.
ALTER TABLE okg.nomos_decisions
  DROP CONSTRAINT IF EXISTS nomos_decisions_decision_id_key;

-- Historical orphans are migration blockers so operators can backfill or delete
-- them deliberately instead of losing referential context.
--
-- make-migrate-idempotent (#669, design ruling 3): the previous version dropped
-- all five composite FKs unconditionally above and re-validated + re-added each
-- one on EVERY migrate — a self-inflicted drop/recreate cycle whose ADD forces
-- a full FK re-validation of the child table against okg.nomos_decisions
-- (hours of downtime on a populated deployment). The fix is a per-child
-- existence guard: if the deployment-scoped composite FK already exists with
-- the right shape (foreign key, arity-2 key, referencing okg.nomos_decisions),
-- skip the drop, the orphan anti-join, and the re-add. Only a genuinely absent
-- or legacy single-column constraint triggers the validate+install path, so the
-- orphan scan is a one-time cutover cost, not a per-run one.
DO $nomos_decision_child_fks$
DECLARE
  child RECORD;
  orphan_count BIGINT;
  orphan_sample TEXT;
  fk_present BOOLEAN;
BEGIN
  FOR child IN
    SELECT *
      FROM (
        VALUES
          (
            'federation_connection_requests',
            'federation_connection_requests_nomos_decision_id_fkey',
            'nomos_decision_deployment_name'
          ),
          (
            'federation_peer_trust_pins',
            'federation_peer_trust_pins_nomos_decision_id_fkey',
            'nomos_decision_deployment_name'
          ),
          (
            'federation_route_events',
            'federation_route_events_nomos_decision_id_fkey',
            'nomos_decision_deployment_name'
          ),
          (
            'poros_incorporation_events',
            'poros_incorporation_events_nomos_decision_id_fkey',
            'nomos_decision_deployment_name'
          ),
          (
            'nomos_external_claims',
            'nomos_external_claims_nomos_decision_id_fkey',
            'deployment_name'
          )
      ) AS child_tables(table_name, constraint_name, deployment_column)
  LOOP
    -- Existence guard: skip children whose correct-shape composite FK already
    -- exists. No drop, no orphan anti-join, no re-add on a converged database.
    SELECT EXISTS (
      SELECT 1
        FROM pg_constraint con
        JOIN pg_class rel ON rel.oid = con.conrelid
        JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
       WHERE nsp.nspname = 'okg'
         AND rel.relname = child.table_name
         AND con.conname = child.constraint_name
         AND con.contype = 'f'
         AND con.confrelid = 'okg.nomos_decisions'::regclass
         AND array_length(con.conkey, 1) = 2
    )
      INTO fk_present;

    IF fk_present THEN
      CONTINUE;
    END IF;

    -- Absent or legacy single-column constraint: replace any prior
    -- single-column FK/global-key experiment before installing the
    -- deployment-scoped contract, then validate + add (the one-time cutover).
    EXECUTE format(
      'ALTER TABLE okg.%I DROP CONSTRAINT IF EXISTS %I',
      child.table_name,
      child.constraint_name
    );

    EXECUTE format(
      'SELECT count(*)
         FROM okg.%I c
    LEFT JOIN okg.nomos_decisions d
           ON d.deployment_name = c.%I
          AND d.decision_id = c.nomos_decision_id
        WHERE c.nomos_decision_id IS NOT NULL
          AND d.decision_id IS NULL',
      child.table_name,
      child.deployment_column
    )
      INTO orphan_count;

    IF orphan_count > 0 THEN
      EXECUTE format(
        'SELECT string_agg(ref, '', '' ORDER BY ref)
           FROM (
             SELECT DISTINCT c.%I || '':'' || c.nomos_decision_id AS ref
               FROM okg.%I c
          LEFT JOIN okg.nomos_decisions d
                 ON d.deployment_name = c.%I
                AND d.decision_id = c.nomos_decision_id
              WHERE c.nomos_decision_id IS NOT NULL
                AND d.decision_id IS NULL
              ORDER BY ref
              LIMIT 10
           ) orphaned',
        child.deployment_column,
        child.table_name,
        child.deployment_column
      )
        INTO orphan_sample;

      RAISE EXCEPTION
        'cannot add okg.% deployment-scoped nomos_decision_id foreign key: % orphan rows exist',
        child.table_name,
        orphan_count
        USING
          ERRCODE = 'foreign_key_violation',
          DETAIL = 'orphaned nomos_decision_id sample: ' ||
            COALESCE(orphan_sample, '<none>'),
          HINT = 'Run `uv run okg doctor --check nomos-decision-referential-integrity --json`, then backfill or delete orphaned child rows before rerunning migration.';
    END IF;

    EXECUTE format(
      'ALTER TABLE okg.%I
         ADD CONSTRAINT %I
         FOREIGN KEY (%I, nomos_decision_id)
         REFERENCES okg.nomos_decisions(deployment_name, decision_id)
         ON DELETE RESTRICT',
      child.table_name,
      child.constraint_name,
      child.deployment_column
    );
  END LOOP;
END
$nomos_decision_child_fks$;

-- Nomos activation decision evidence became mandatory after early installs
-- allowed NULL activation_decision_id values. Fresh tables declare NOT NULL
-- above; this block upgrades existing deployments by recording explicit allow
-- decisions for legacy nullable rows, then enforcing the table constraint.
DO $nomos_activation_decision_backfill$
BEGIN
  IF EXISTS (
    SELECT 1
      FROM information_schema.columns
     WHERE table_schema = 'okg'
       AND table_name = 'nomos_policy_activations'
       AND column_name = 'activation_decision_id'
       AND is_nullable = 'YES'
  ) THEN
    INSERT INTO okg.nomos_decisions (
      deployment_name, decision_id, policy_bundle_id, policy_version,
      policy_fingerprint, input_hash, subject, action, resource,
      classification, context, effect, reason_code, matched_rule_ids,
      obligations, enforcement_point, target
    )
    SELECT
      a.deployment_name,
      'nomos:decision:migration:policy_activation:' ||
        md5(a.deployment_name || ':' || a.activation_id),
      b.policy_bundle_id,
      b.policy_version,
      a.policy_fingerprint,
      'md5:' || md5(a.deployment_name || ':' || a.activation_id),
      a.activated_by,
      'policy.activate',
      jsonb_build_object(
        'deployment', a.deployment_name,
        'scope', a.scope,
        'policy_fingerprint', a.policy_fingerprint,
        'activation_id', a.activation_id
      ),
      'internal',
      jsonb_build_object(
        'migration', 'nomos-activation-decision-required'
      ),
      'allow',
      'nomos.migration.activation_decision_backfill',
      ARRAY['migration_backfill']::TEXT[],
      '[{"type":"audit"}]'::JSONB,
      'policy.activate',
      jsonb_build_object(
        'kind', 'policy_activation',
        'activation_path', 'migration_backfill',
        'activation_id', a.activation_id
      )
      FROM okg.nomos_policy_activations a
      LEFT JOIN okg.nomos_policy_bundles b
        ON b.deployment_name = a.deployment_name
       AND b.policy_fingerprint = a.policy_fingerprint
     WHERE a.activation_decision_id IS NULL
    ON CONFLICT (deployment_name, decision_id) DO NOTHING;

    UPDATE okg.nomos_policy_activations a
       SET activation_decision_id =
           'nomos:decision:migration:policy_activation:' ||
           md5(a.deployment_name || ':' || a.activation_id)
     WHERE a.activation_decision_id IS NULL;

    IF EXISTS (
      SELECT 1
        FROM okg.nomos_policy_activations a
        LEFT JOIN okg.nomos_decisions d
          ON d.deployment_name = a.deployment_name
         AND d.decision_id = a.activation_decision_id
       WHERE a.activation_decision_id IS NULL
          OR d.effect IS DISTINCT FROM 'allow'
    ) THEN
      RAISE EXCEPTION
        'cannot enforce non-null policy activation decisions';
    END IF;

    ALTER TABLE okg.nomos_policy_activations
      ALTER COLUMN activation_decision_id SET NOT NULL;
  END IF;

  IF EXISTS (
    SELECT 1
      FROM information_schema.columns
     WHERE table_schema = 'okg'
       AND table_name = 'nomos_config_activations'
       AND column_name = 'activation_decision_id'
       AND is_nullable = 'YES'
  ) THEN
    INSERT INTO okg.nomos_decisions (
      deployment_name, decision_id, policy_bundle_id, policy_version,
      policy_fingerprint, input_hash, subject, action, resource,
      classification, context, effect, reason_code, matched_rule_ids,
      obligations, enforcement_point, target
    )
    SELECT
      a.deployment_name,
      'nomos:decision:migration:config_activation:' ||
        md5(a.deployment_name || ':' || a.activation_id),
      NULL,
      NULL,
      NULL,
      'md5:' || md5(a.deployment_name || ':' || a.activation_id),
      a.activated_by,
      'config.activate',
      jsonb_build_object(
        'deployment', a.deployment_name,
        'scope', a.scope,
        'config_fingerprint', a.config_fingerprint,
        'activation_id', a.activation_id
      ),
      'internal',
      jsonb_build_object(
        'migration', 'nomos-activation-decision-required'
      ),
      'allow',
      'nomos.migration.activation_decision_backfill',
      ARRAY['migration_backfill']::TEXT[],
      '[{"type":"audit"}]'::JSONB,
      'config.activate',
      jsonb_build_object(
        'kind', 'config_activation',
        'activation_path', 'migration_backfill',
        'activation_id', a.activation_id
      )
      FROM okg.nomos_config_activations a
     WHERE a.activation_decision_id IS NULL
    ON CONFLICT (deployment_name, decision_id) DO NOTHING;

    UPDATE okg.nomos_config_activations a
       SET activation_decision_id =
           'nomos:decision:migration:config_activation:' ||
           md5(a.deployment_name || ':' || a.activation_id)
     WHERE a.activation_decision_id IS NULL;

    IF EXISTS (
      SELECT 1
        FROM okg.nomos_config_activations a
        LEFT JOIN okg.nomos_decisions d
          ON d.deployment_name = a.deployment_name
         AND d.decision_id = a.activation_decision_id
       WHERE a.activation_decision_id IS NULL
          OR d.effect IS DISTINCT FROM 'allow'
    ) THEN
      RAISE EXCEPTION
        'cannot enforce non-null config activation decisions';
    END IF;

    ALTER TABLE okg.nomos_config_activations
      ALTER COLUMN activation_decision_id SET NOT NULL;
  END IF;
END
$nomos_activation_decision_backfill$;

-- Nomos decision identity dropped the volatile session fields (generation_id
-- / branch_name / catalog_version_id) from the identity-bearing resource
-- (issue #635): legacy rows re-minted the same access decision once per
-- publish, defeating the (deployment_name, decision_id) dedup. Collapse
-- legacy duplicates to the newest representative per
-- identity-without-volatile-keys. Scoped to rows still carrying a volatile
-- key, so legacy-vs-new-scheme pairs are never collapsed and survivors keep
-- their recorded decision_id/resource untouched (the log stays an honest
-- record of what was evaluated). Rows referenced by ANY of the seven
-- referencing tables are never deleted (explicit NOT EXISTS; the FK RESTRICT
-- constraints are a backstop, not the mechanism, so this block can never
-- abort `okg up`). Retention (okg.substrate.nomos.retention) ages the
-- remaining legacy singletons out later.
DO $nomos_decision_volatile_identity_dedup$
BEGIN
  -- BEGIN nomos-decision-volatile-identity-dedup-statement
  WITH legacy AS MATERIALIZED (
    SELECT
      decision_row_id,
      deployment_name,
      decision_id,
      subject,
      action,
      classification,
      effect,
      reason_code,
      enforcement_point,
      coalesce(policy_fingerprint, '') AS policy_fingerprint,
      context,
      resource - 'generation_id' - 'branch_name' - 'catalog_version_id'
        AS stable_resource,
      created_at,
      md5(jsonb_build_array(
        deployment_name,
        subject,
        action,
        classification,
        effect,
        reason_code,
        enforcement_point,
        coalesce(policy_fingerprint, ''),
        context,
        resource - 'generation_id' - 'branch_name' - 'catalog_version_id'
      )::text) AS identity_digest
    FROM okg.nomos_decisions
    WHERE resource ?| ARRAY[
      'generation_id', 'branch_name', 'catalog_version_id'
    ]
  ),
  ranked AS MATERIALIZED (
    SELECT
      decision_row_id,
      row_number() OVER (
        PARTITION BY
          identity_digest,
          deployment_name,
          subject,
          action,
          classification,
          effect,
          reason_code,
          enforcement_point,
          policy_fingerprint,
          context,
          stable_resource
        ORDER BY created_at DESC, decision_row_id DESC
      ) AS identity_rank
    FROM legacy
  )
  DELETE FROM okg.nomos_decisions d
  USING ranked
  WHERE d.decision_row_id = ranked.decision_row_id
    AND ranked.identity_rank > 1
    AND NOT EXISTS (
      SELECT 1 FROM okg.federation_connection_requests r
       WHERE r.nomos_decision_deployment_name = d.deployment_name
         AND r.nomos_decision_id = d.decision_id
    )
    AND NOT EXISTS (
      SELECT 1 FROM okg.federation_peer_trust_pins r
       WHERE r.nomos_decision_deployment_name = d.deployment_name
         AND r.nomos_decision_id = d.decision_id
    )
    AND NOT EXISTS (
      SELECT 1 FROM okg.federation_route_events r
       WHERE r.nomos_decision_deployment_name = d.deployment_name
         AND r.nomos_decision_id = d.decision_id
    )
    AND NOT EXISTS (
      SELECT 1 FROM okg.poros_incorporation_events r
       WHERE r.nomos_decision_deployment_name = d.deployment_name
         AND r.nomos_decision_id = d.decision_id
    )
    AND NOT EXISTS (
      SELECT 1 FROM okg.nomos_external_claims r
       WHERE r.deployment_name = d.deployment_name
         AND r.nomos_decision_id = d.decision_id
    )
    AND NOT EXISTS (
      SELECT 1 FROM okg.nomos_policy_activations r
       WHERE r.deployment_name = d.deployment_name
         AND r.activation_decision_id = d.decision_id
    )
    AND NOT EXISTS (
      SELECT 1 FROM okg.nomos_config_activations r
       WHERE r.deployment_name = d.deployment_name
         AND r.activation_decision_id = d.decision_id
    );
  -- END nomos-decision-volatile-identity-dedup-statement
END
$nomos_decision_volatile_identity_dedup$;

-- ========== Substrate branches (add-akmon-branch-and-quarantine Phase A) ==========
--
-- A branch is the substrate's lineage unit. Phase A defines the
-- schema (this section) and an auto-fill trigger so that the
-- existing publisher (branch-unaware) continues to insert new
-- `graph_generations` rows without modification: any row whose
-- `branch_id` is NULL gets its deployment's `default` branch
-- materialized (idempotently) and assigned. Phase A.2 wires the
-- publisher to write `branch_id` directly and promotes the column
-- to NOT NULL plus the per-(deployment, branch) pending uniqueness.
--
-- The branches table is created here, BEFORE the bootstrap
-- generation insert below, so the trigger can resolve branch_id
-- for the bootstrap row on a fresh DB.

CREATE TABLE IF NOT EXISTS okg.branches (
  branch_id           BIGSERIAL PRIMARY KEY,
  deployment_name     TEXT      NOT NULL,
  name                TEXT      NOT NULL,
  role                TEXT      NOT NULL
    CHECK (role IN ('default','quarantine','experiment')),
  -- Lineage FKs are ON DELETE SET NULL so the branches table never
  -- blocks an upstream-generation or upstream-branch deletion. A
  -- branch whose base disappears keeps existing as an orphan
  -- (status remains active until an operator explicitly abandons
  -- it); base_branch_id / base_generation_id are advisory lineage
  -- anchors, not load-bearing identity.
  base_branch_id      BIGINT
    REFERENCES okg.branches(branch_id) ON DELETE SET NULL,
  base_generation_id  BIGINT,
  status              TEXT      NOT NULL
    CHECK (status IN ('active','merged','abandoned','superseded')),
  policy_clauses      JSONB     NOT NULL DEFAULT '{}'::jsonb,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  closed_at           TIMESTAMPTZ,
  metadata            JSONB     NOT NULL DEFAULT '{}'::jsonb
);

-- Self-healing for existing deployments: drop the legacy FK that
-- omits ON DELETE SET NULL and recreate with the new behavior.
DO $branches_lineage_fk_setnull$
BEGIN
  IF EXISTS (
    SELECT 1
      FROM pg_constraint c
      JOIN pg_class t ON t.oid = c.conrelid
      JOIN pg_namespace n ON n.oid = t.relnamespace
     WHERE n.nspname = 'okg'
       AND t.relname = 'branches'
       AND c.conname = 'branches_base_branch_id_fkey'
       AND c.confdeltype <> 'n'
  ) THEN
    ALTER TABLE okg.branches
      DROP CONSTRAINT branches_base_branch_id_fkey;
    ALTER TABLE okg.branches
      ADD CONSTRAINT branches_base_branch_id_fkey
      FOREIGN KEY (base_branch_id)
      REFERENCES okg.branches(branch_id) ON DELETE SET NULL;
  END IF;
  ALTER TABLE okg.branches
    DROP CONSTRAINT IF EXISTS branches_base_generation_id_fkey;
END
$branches_lineage_fk_setnull$;

CREATE UNIQUE INDEX IF NOT EXISTS branches_deployment_name_uidx
  ON okg.branches (deployment_name, name);

CREATE INDEX IF NOT EXISTS branches_deployment_role_status_idx
  ON okg.branches (deployment_name, role, status, branch_id);

-- Add branch_id column to graph_generations (FK to branches).
-- Idempotent + self-healing: ADD COLUMN IF NOT EXISTS, then
-- ALTER COLUMN DROP NOT NULL in case a previous migration attempt
-- left the column NOT NULL (which would break the bootstrap insert
-- below until the trigger populates it).
ALTER TABLE okg.graph_generations
  ADD COLUMN IF NOT EXISTS branch_id BIGINT
    REFERENCES okg.branches(branch_id);

ALTER TABLE okg.graph_generations
  ALTER COLUMN branch_id DROP NOT NULL;

-- Auto-fill trigger: any INSERT (and UPDATE that touches
-- deployment_name or branch_id) without an explicit branch_id
-- gets the deployment's `default` branch resolved (materialized
-- if absent). Keeps the existing branch-unaware publisher working.
CREATE OR REPLACE FUNCTION okg._fill_default_branch_id()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
  IF NEW.branch_id IS NULL AND NEW.deployment_name IS NOT NULL THEN
    INSERT INTO okg.branches
      (deployment_name, name, role, status, metadata)
    VALUES
      (NEW.deployment_name, 'default', 'default', 'active',
       '{"auto_materialized":"_fill_default_branch_id trigger"}'::jsonb)
    ON CONFLICT (deployment_name, name) DO NOTHING;
    SELECT branch_id INTO NEW.branch_id
      FROM okg.branches
     WHERE deployment_name = NEW.deployment_name
       AND name = 'default'
     LIMIT 1;
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS graph_generations_fill_branch_id_tg
  ON okg.graph_generations;

CREATE TRIGGER graph_generations_fill_branch_id_tg
  BEFORE INSERT OR UPDATE OF deployment_name, branch_id
  ON okg.graph_generations
  FOR EACH ROW
  EXECUTE FUNCTION okg._fill_default_branch_id();

CREATE INDEX IF NOT EXISTS graph_generations_branch_status_idx
  ON okg.graph_generations (branch_id, status, generation_id DESC);

-- Formal bootstrap generation. Generation 0 is used by a few
-- bootstrap/test paths; make it a real ledger row so live-table FK
-- constraints do not rely on convention.
INSERT INTO okg.graph_generations
  (generation_id, deployment_name, status, catalog_version_id,
   published_at, correlation_id, invariant_summary, excluded_sources)
VALUES
  (0, 'bootstrap', 'published', 1, now(), 'bootstrap:generation:0',
   '{"reason":"bootstrap generation zero"}'::jsonb, '[]'::jsonb)
ON CONFLICT (generation_id) DO NOTHING;

SELECT setval(
  pg_get_serial_sequence('okg.graph_generations', 'generation_id'),
  GREATEST(1, (SELECT max(generation_id) FROM okg.graph_generations))
);

CREATE INDEX IF NOT EXISTS graph_generations_status_published_idx
  ON okg.graph_generations (status, published_at DESC);
CREATE INDEX IF NOT EXISTS graph_generations_deployment_status_idx
  ON okg.graph_generations (deployment_name, status, generation_id DESC);

-- Enforce "exactly one pending generation per deployment" in the
-- schema. Combined with the per-deployment advisory lock this
-- gives a real invariant, not a publisher-side hope.
--
-- Phase A.2 superseded this per-deployment uniqueness with a
-- per-(deployment, branch_id) variant (created in the branches
-- section below).  The conditional-create avoids re-creating this
-- index on idempotent re-migration: if the branch-aware uidx is
-- already present, the per-deployment one is dead weight.  On a
-- fresh DB the branch-aware index doesn't exist yet, so this
-- creates the legacy guarantee, and the branches section's
-- DROP INDEX later in the schema swaps it cleanly.
DO $pending_per_dep_uidx_create_if_no_branch_uidx$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_indexes
     WHERE schemaname = 'okg'
       AND indexname =
         'graph_generations_one_pending_per_branch_uidx'
  ) THEN
    CREATE UNIQUE INDEX IF NOT EXISTS
      graph_generations_one_pending_per_dep_uidx
      ON okg.graph_generations (deployment_name)
      WHERE status = 'pending';
  END IF;
END
$pending_per_dep_uidx_create_if_no_branch_uidx$;

-- Materialize a 'default' branch for every deployment that
-- already has graph_generations rows. The trigger above only
-- fires on INSERT/UPDATE-of-deployment-or-branch_id; pre-
-- existing rows from older migrations never went through it,
-- so their deployment's default branch must be created
-- explicitly before the backfill below can resolve it. Without
-- this step, deployments upgrading from pre-A.2 schemas fail
-- the `SET NOT NULL` further down.
INSERT INTO okg.branches
  (deployment_name, name, role, status, metadata)
SELECT DISTINCT
       gg.deployment_name, 'default', 'default', 'active',
       '{"auto_materialized":"schema_backfill_default_branch"}'::jsonb
  FROM okg.graph_generations gg
 WHERE gg.deployment_name IS NOT NULL
ON CONFLICT (deployment_name, name) DO NOTHING;

-- Backfill branch_id for any pre-existing rows that predate the
-- branches section above. Idempotent: a fresh DB has no rows; a
-- DB whose graph_generations rows already have branch_id set is
-- left alone.
UPDATE okg.graph_generations gg
   SET branch_id = b.branch_id
  FROM okg.branches b
 WHERE b.deployment_name = gg.deployment_name
   AND b.name = 'default'
   AND gg.branch_id IS NULL;

-- Phase A.2: promote branch_id to NOT NULL now that the trigger
-- guarantees every insert resolves a branch_id. Idempotent (the
-- ALTER is a no-op if already NOT NULL).
ALTER TABLE okg.graph_generations
  ALTER COLUMN branch_id SET NOT NULL;

-- Phase A.2: swap the per-deployment pending uniqueness for a
-- per-(deployment, branch) one. Two pending generations on the
-- same branch remain forbidden, but concurrent publishes on
-- different branches of the same deployment are now allowed.
DROP INDEX IF EXISTS okg.graph_generations_one_pending_per_dep_uidx;

CREATE UNIQUE INDEX IF NOT EXISTS graph_generations_one_pending_per_branch_uidx
  ON okg.graph_generations (deployment_name, branch_id)
  WHERE status = 'pending';

-- ========== Substrate branches (Phase A.3 — live tables) ==========
--
-- ========== Branch merge requests (Phase C foundation) ==========
--
-- A merge request is an operator's (or workflow's) intent to fold
-- a non-default branch back into the default branch (or into
-- another branch). Phase C.1 (this section) ships the durable
-- record so doctor can enforce "any branch with status='merged'
-- has an approved merge_request"; the actual merge applier — the
-- code that routes the source-branch's facts back through
-- admission on the target branch, gated by a merge_decision
-- authority profile — is Phase C.2.

CREATE TABLE IF NOT EXISTS okg.branch_merge_requests (
  request_id          BIGSERIAL PRIMARY KEY,
  deployment_name     TEXT      NOT NULL,
  source_branch_id    BIGINT    NOT NULL
    REFERENCES okg.branches(branch_id) ON DELETE CASCADE,
  target_branch_id    BIGINT    NOT NULL
    REFERENCES okg.branches(branch_id) ON DELETE CASCADE,
  status              TEXT      NOT NULL
    CHECK (status IN (
      'pending', 'approved', 'rejected', 'applied', 'withdrawn'
    )),
  predicate           JSONB     NOT NULL DEFAULT '{}'::jsonb,
  verdict             JSONB     NOT NULL DEFAULT '{}'::jsonb,
  decided_by          TEXT,
  decided_at          TIMESTAMPTZ,
  applied_at          TIMESTAMPTZ,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  metadata            JSONB     NOT NULL DEFAULT '{}'::jsonb,
  CHECK (source_branch_id <> target_branch_id)
);

CREATE INDEX IF NOT EXISTS branch_merge_requests_source_idx
  ON okg.branch_merge_requests
     (deployment_name, source_branch_id, status, created_at DESC);
CREATE INDEX IF NOT EXISTS branch_merge_requests_target_idx
  ON okg.branch_merge_requests
     (deployment_name, target_branch_id, status, created_at DESC);

-- ========== Derived artifact metadata ==========
--
-- These tables are the V1 spine for derived artifacts: views,
-- consistency checks, and future derived outputs all publish through
-- an atomic output-set manifest. Physical relation payloads live in
-- the substrate-owned `okg_derived` schema and are referenced from
-- manifest slots; they are not exposed to app_ro unless a pinned
-- okg.v_* read view is added deliberately.

CREATE TABLE IF NOT EXISTS okg.derived_artifact_definitions (
  definition_id       TEXT PRIMARY KEY,
  artifact_kind       TEXT NOT NULL
    CHECK (artifact_kind IN (
      'derived_artifact', 'view', 'consistency_check', 'lens'
    )),
  deployment_name     TEXT NOT NULL,
  logical_key         TEXT NOT NULL,
  compute_hash        TEXT NOT NULL,
  output_schema_hash  TEXT NOT NULL,
  renderer_hash       TEXT NOT NULL,
  dirty_rule_hash     TEXT NOT NULL,
  renderer_ref        TEXT NOT NULL,
  output_contract     JSONB NOT NULL DEFAULT '{}'::jsonb,
  dirty_rule          JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

DO $derived_artifact_definitions_artifact_kind_check$
BEGIN
  ALTER TABLE okg.derived_artifact_definitions
    DROP CONSTRAINT IF EXISTS derived_artifact_definitions_artifact_kind_check;
  ALTER TABLE okg.derived_artifact_definitions
    ADD CONSTRAINT derived_artifact_definitions_artifact_kind_check
    CHECK (
      artifact_kind IN (
        'derived_artifact', 'view', 'consistency_check', 'lens'
      )
    );
END
$derived_artifact_definitions_artifact_kind_check$;

CREATE INDEX IF NOT EXISTS derived_artifact_definitions_logical_idx
  ON okg.derived_artifact_definitions
  (deployment_name, artifact_kind, logical_key);

CREATE TABLE IF NOT EXISTS okg.derived_artifact_runs (
  run_id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  definition_id             TEXT NOT NULL
    REFERENCES okg.derived_artifact_definitions(definition_id),
  target_generation_id      BIGINT NOT NULL,
  status                    TEXT NOT NULL
    CHECK (status IN (
      'running', 'completed', 'failed', 'carried_forward', 'blocked'
    )),
  trigger_kind              TEXT NOT NULL
    CHECK (trigger_kind IN (
      'dirty', 'full', 'carry_forward', 'manual', 'migration',
      'post_publish'
    )),
  compute_hash              TEXT NOT NULL,
  output_schema_hash        TEXT NOT NULL,
  renderer_hash             TEXT NOT NULL,
  dirty_rule_hash           TEXT NOT NULL,
  dependency_manifest_ids   UUID[] NOT NULL DEFAULT '{}'::uuid[],
  output_manifest_id        UUID,
  reused_from_run_id        UUID
    REFERENCES okg.derived_artifact_runs(run_id),
  total_scopes              BIGINT NOT NULL DEFAULT 0,
  dirty_scopes              BIGINT NOT NULL DEFAULT 0,
  dirty_rows                BIGINT NOT NULL DEFAULT 0,
  reused_scopes             BIGINT NOT NULL DEFAULT 0,
  fallback_count            BIGINT NOT NULL DEFAULT 0,
  metrics                   JSONB NOT NULL DEFAULT '{}'::jsonb,
  error                     JSONB,
  started_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
  completed_at              TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS derived_artifact_runs_target_idx
  ON okg.derived_artifact_runs
  (definition_id, target_generation_id, status);

CREATE INDEX IF NOT EXISTS derived_artifact_runs_completed_idx
  ON okg.derived_artifact_runs
  (definition_id, completed_at DESC)
  WHERE status IN ('completed', 'carried_forward');

DO $derived_artifact_runs_trigger_kind_check$
BEGIN
  ALTER TABLE okg.derived_artifact_runs
    DROP CONSTRAINT IF EXISTS derived_artifact_runs_trigger_kind_check;
  ALTER TABLE okg.derived_artifact_runs
    ADD CONSTRAINT derived_artifact_runs_trigger_kind_check
    CHECK (
      trigger_kind IN (
        'dirty', 'full', 'carry_forward', 'manual', 'migration',
        'post_publish'
      )
    );
END
$derived_artifact_runs_trigger_kind_check$;

CREATE TABLE IF NOT EXISTS okg.derived_output_manifests (
  manifest_id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  run_id                   UUID NOT NULL
    REFERENCES okg.derived_artifact_runs(run_id),
  definition_id            TEXT NOT NULL
    REFERENCES okg.derived_artifact_definitions(definition_id),
  target_generation_id     BIGINT NOT NULL,
  status                   TEXT NOT NULL
    CHECK (status IN ('staged', 'completed', 'failed')),
  required_slots           TEXT[] NOT NULL DEFAULT '{}'::text[],
  manifest_hash            TEXT NOT NULL,
  reused_from_manifest_id  UUID
    REFERENCES okg.derived_output_manifests(manifest_id),
  metadata                 JSONB NOT NULL DEFAULT '{}'::jsonb,
  staged_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
  promoted_at              TIMESTAMPTZ,
  failed_at                TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS derived_output_manifests_completed_uidx
  ON okg.derived_output_manifests
  (definition_id, target_generation_id)
  WHERE status = 'completed';

CREATE INDEX IF NOT EXISTS derived_output_manifests_target_idx
  ON okg.derived_output_manifests
  (definition_id, target_generation_id, status);

CREATE INDEX IF NOT EXISTS derived_output_manifests_completed_latest_idx
  ON okg.derived_output_manifests
  (definition_id, target_generation_id DESC, promoted_at DESC)
  INCLUDE (manifest_id, run_id)
  WHERE status = 'completed';

CREATE TABLE IF NOT EXISTS okg.derived_output_slots (
  slot_id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  manifest_id       UUID NOT NULL
    REFERENCES okg.derived_output_manifests(manifest_id)
    ON DELETE CASCADE,
  slot_name         TEXT NOT NULL,
  storage_kind      TEXT NOT NULL
    CHECK (storage_kind IN (
      'relation', 'inline', 'graph_projection', 'compatibility'
    )),
  media_type        TEXT NOT NULL,
  artifact_type     TEXT NOT NULL,
  renderer_ref      TEXT NOT NULL,
  payload_hash      TEXT NOT NULL,
  storage_ref       JSONB NOT NULL,
  inline_payload    JSONB,
  row_count         BIGINT,
  byte_count        BIGINT,
  metadata          JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT derived_output_slots_inline_payload_chk
    CHECK (storage_kind <> 'inline' OR inline_payload IS NOT NULL),
  UNIQUE (manifest_id, slot_name)
);

ALTER TABLE okg.derived_output_slots
  ADD COLUMN IF NOT EXISTS inline_payload JSONB;

DO $derived_output_slots_inline_payload_chk$
BEGIN
  IF NOT EXISTS (
    SELECT 1
      FROM pg_constraint
     WHERE connamespace = 'okg'::regnamespace
       AND conrelid = 'okg.derived_output_slots'::regclass
       AND conname = 'derived_output_slots_inline_payload_chk'
  ) THEN
    ALTER TABLE okg.derived_output_slots
      ADD CONSTRAINT derived_output_slots_inline_payload_chk
      CHECK (storage_kind <> 'inline' OR inline_payload IS NOT NULL);
  END IF;
END
$derived_output_slots_inline_payload_chk$;

CREATE INDEX IF NOT EXISTS derived_output_slots_manifest_idx
  ON okg.derived_output_slots (manifest_id, storage_kind);

CREATE TABLE IF NOT EXISTS okg.derived_output_lineage_envelopes (
  lineage_envelope_id  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  manifest_id          UUID NOT NULL
    REFERENCES okg.derived_output_manifests(manifest_id)
    ON DELETE CASCADE,
  definition_id        TEXT NOT NULL
    REFERENCES okg.derived_artifact_definitions(definition_id),
  target_generation_id BIGINT NOT NULL,
  output_id            TEXT NOT NULL,
  output_kind          TEXT NOT NULL,
  lineage_schema       TEXT NOT NULL,
  envelope_hash        TEXT NOT NULL,
  output_digest        TEXT NOT NULL,
  previous_output_digest TEXT,
  component_id         TEXT NOT NULL,
  component_hash       TEXT NOT NULL,
  invalidation_scope   TEXT NOT NULL,
  envelope             JSONB NOT NULL,
  created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (manifest_id, output_id)
);

CREATE INDEX IF NOT EXISTS derived_output_lineage_manifest_idx
  ON okg.derived_output_lineage_envelopes
  (manifest_id, output_id);

CREATE INDEX IF NOT EXISTS derived_output_lineage_target_idx
  ON okg.derived_output_lineage_envelopes
  (definition_id, target_generation_id, output_kind);

CREATE TABLE IF NOT EXISTS okg.derived_output_dependency_index (
  dependency_index_id  BIGSERIAL PRIMARY KEY,
  lineage_envelope_id  UUID NOT NULL
    REFERENCES okg.derived_output_lineage_envelopes(lineage_envelope_id)
    ON DELETE CASCADE,
  manifest_id          UUID NOT NULL
    REFERENCES okg.derived_output_manifests(manifest_id)
    ON DELETE CASCADE,
  output_id            TEXT NOT NULL,
  dependency_kind      TEXT NOT NULL,
  dependency_ref       TEXT NOT NULL,
  dependency_key_hash  TEXT NOT NULL,
  created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (
    lineage_envelope_id, dependency_kind, dependency_ref
  )
);

CREATE INDEX IF NOT EXISTS derived_output_dependency_lookup_idx
  ON okg.derived_output_dependency_index
  (dependency_kind, dependency_ref, manifest_id);

CREATE INDEX IF NOT EXISTS derived_output_dependency_manifest_idx
  ON okg.derived_output_dependency_index
  (manifest_id, output_id);

CREATE TABLE IF NOT EXISTS okg.derived_relation_registry (
  relation_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  definition_id       TEXT NOT NULL
    REFERENCES okg.derived_artifact_definitions(definition_id),
  logical_relation_id TEXT NOT NULL,
  schema_name         TEXT NOT NULL DEFAULT 'okg_derived',
  physical_name       TEXT NOT NULL,
  output_schema_hash  TEXT NOT NULL,
  ddl_hash            TEXT NOT NULL,
  partition_policy    JSONB NOT NULL DEFAULT '{}'::jsonb,
  index_policy        JSONB NOT NULL DEFAULT '{}'::jsonb,
  retention_policy    JSONB NOT NULL DEFAULT '{}'::jsonb,
  status              TEXT NOT NULL
    CHECK (status IN ('active', 'retired')),
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  retired_at          TIMESTAMPTZ,
  UNIQUE (schema_name, physical_name),
  UNIQUE (definition_id, logical_relation_id, output_schema_hash)
);

CREATE INDEX IF NOT EXISTS derived_relation_registry_definition_idx
  ON okg.derived_relation_registry
  (definition_id, logical_relation_id, status);

CREATE TABLE IF NOT EXISTS okg.derived_dirty_rule_runs (
  dirty_rule_run_id    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  definition_id        TEXT NOT NULL
    REFERENCES okg.derived_artifact_definitions(definition_id),
  target_generation_id BIGINT NOT NULL,
  dirty_rule_hash      TEXT NOT NULL,
  scope_universe_hash  TEXT NOT NULL,
  dirty_query_hash     TEXT NOT NULL,
  audit_query_hash     TEXT,
  status               TEXT NOT NULL
    CHECK (status IN ('running', 'completed', 'failed', 'audit_failed')),
  fallback_kind        TEXT NOT NULL DEFAULT 'none'
    CHECK (fallback_kind IN ('none', 'full_scope_universe')),
  total_scope_count    BIGINT NOT NULL DEFAULT 0
    CHECK (total_scope_count >= 0),
  dirty_scope_count    BIGINT NOT NULL DEFAULT 0
    CHECK (dirty_scope_count >= 0),
  fallback_scope_count BIGINT NOT NULL DEFAULT 0
    CHECK (fallback_scope_count >= 0),
  missed_scope_count   BIGINT NOT NULL DEFAULT 0
    CHECK (missed_scope_count >= 0),
  elapsed_ms           BIGINT NOT NULL DEFAULT 0
    CHECK (elapsed_ms >= 0),
  metrics              JSONB NOT NULL DEFAULT '{}'::jsonb,
  error                JSONB,
  started_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  completed_at         TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS derived_dirty_rule_runs_target_idx
  ON okg.derived_dirty_rule_runs
  (definition_id, target_generation_id, status);

CREATE TABLE IF NOT EXISTS okg.derived_dirty_scopes (
  dirty_scope_id       BIGSERIAL PRIMARY KEY,
  dirty_rule_run_id    UUID
    REFERENCES okg.derived_dirty_rule_runs(dirty_rule_run_id),
  definition_id        TEXT NOT NULL
    REFERENCES okg.derived_artifact_definitions(definition_id),
  target_generation_id BIGINT NOT NULL,
  scope_key            TEXT NOT NULL,
  dirty_reason         TEXT NOT NULL,
  dependency_kind      TEXT NOT NULL,
  dependency_ref       TEXT NOT NULL,
  dependency_hash      TEXT,
  queued_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
  consumed_run_id      UUID
    REFERENCES okg.derived_artifact_runs(run_id),
  consumed_at          TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS derived_dirty_scopes_dedupe_uidx
  ON okg.derived_dirty_scopes
  (definition_id, target_generation_id, scope_key,
   dependency_kind, dependency_ref);

CREATE INDEX IF NOT EXISTS derived_dirty_scopes_unconsumed_idx
  ON okg.derived_dirty_scopes
  (definition_id, target_generation_id, queued_at)
  WHERE consumed_run_id IS NULL;

ALTER TABLE IF EXISTS okg.derived_dirty_scopes
  ADD COLUMN IF NOT EXISTS dirty_rule_run_id UUID;

DO $derived_dirty_scopes_dirty_rule_run_fk$
BEGIN
  IF NOT EXISTS (
    SELECT 1
      FROM pg_constraint
     WHERE conname = 'derived_dirty_scopes_dirty_rule_run_id_fkey'
       AND conrelid = 'okg.derived_dirty_scopes'::regclass
  ) THEN
    ALTER TABLE okg.derived_dirty_scopes
      ADD CONSTRAINT derived_dirty_scopes_dirty_rule_run_id_fkey
      FOREIGN KEY (dirty_rule_run_id)
      REFERENCES okg.derived_dirty_rule_runs(dirty_rule_run_id);
  END IF;
END
$derived_dirty_scopes_dirty_rule_run_fk$;

CREATE TABLE IF NOT EXISTS okg.derived_dirty_rule_audits (
  audit_id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  dirty_rule_run_id    UUID NOT NULL
    REFERENCES okg.derived_dirty_rule_runs(dirty_rule_run_id)
    ON DELETE CASCADE,
  definition_id        TEXT NOT NULL
    REFERENCES okg.derived_artifact_definitions(definition_id),
  target_generation_id BIGINT NOT NULL,
  audit_kind           TEXT NOT NULL
    CHECK (audit_kind IN ('missed_dirty')),
  status               TEXT NOT NULL
    CHECK (status IN ('passed', 'failed', 'skipped')),
  expected_scope_count BIGINT NOT NULL DEFAULT 0
    CHECK (expected_scope_count >= 0),
  observed_scope_count BIGINT NOT NULL DEFAULT 0
    CHECK (observed_scope_count >= 0),
  missed_scope_count   BIGINT NOT NULL DEFAULT 0
    CHECK (missed_scope_count >= 0),
  missed_scope_keys    JSONB NOT NULL DEFAULT '[]'::jsonb,
  metadata             JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS derived_dirty_rule_audits_target_idx
  ON okg.derived_dirty_rule_audits
  (definition_id, target_generation_id, status);

CREATE TABLE IF NOT EXISTS okg.consistency_check_definitions (
  check_id             TEXT PRIMARY KEY
    REFERENCES okg.derived_artifact_definitions(definition_id),
  deployment_name      TEXT NOT NULL,
  predicate_family     TEXT NOT NULL,
  candidate_rule_hash  TEXT NOT NULL,
  evaluator_ref        TEXT NOT NULL,
  evaluator_hash       TEXT NOT NULL,
  confidence_type      TEXT NOT NULL
    CHECK (confidence_type IN (
      'deterministic_boolean',
      'calibrated_probability',
      'thresholded_score',
      'ordinal_label'
    )),
  coverage_contract    JSONB NOT NULL DEFAULT '{}'::jsonb,
  severity_policy      JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS consistency_check_definitions_dep_idx
  ON okg.consistency_check_definitions
  (deployment_name, predicate_family);

-- AKMON Phase 7.1 (umbrella align-akmon-implementation-to-vision):
-- the four-kind integrity taxonomy used by the vision paper to
-- classify checks by *what kind of evidence they verify*.
--   * rule_based   — deterministic catalog/narrowing rules (the
--                    admission-time rejections handled by the
--                    producer-authority signature evaluator).
--   * security     — content / policy gates (e.g. banned strings,
--                    PII redaction). Run pre-publish; can block.
--   * statistical  — drift / coverage / distribution checks. Run
--                    post-publish as observations; do not block.
--   * semantic     — LLM-judge invariants. Run any phase; observe.
-- predicate_family stays as the narrower per-check id.
-- Existing rows keep check_kind=NULL; new registrations should
-- supply one of the four values. Doctor surfaces NULL rows so
-- deployments can audit their coverage.
ALTER TABLE okg.consistency_check_definitions
  ADD COLUMN IF NOT EXISTS check_kind TEXT;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conrelid = 'okg.consistency_check_definitions'::regclass
       AND conname = 'consistency_check_definitions_kind_check'
  ) THEN
    ALTER TABLE okg.consistency_check_definitions
      ADD CONSTRAINT consistency_check_definitions_kind_check
      CHECK (
        check_kind IS NULL OR check_kind IN (
          'rule_based', 'security', 'statistical', 'semantic'
        )
      );
  END IF;
END
$$;

CREATE INDEX IF NOT EXISTS consistency_check_definitions_kind_idx
  ON okg.consistency_check_definitions (deployment_name, check_kind)
  WHERE check_kind IS NOT NULL;

CREATE TABLE IF NOT EXISTS okg.consistency_candidates (
  check_id              TEXT NOT NULL
    REFERENCES okg.consistency_check_definitions(check_id),
  target_generation_id  BIGINT NOT NULL,
  candidate_id          TEXT NOT NULL,
  run_id                UUID
    REFERENCES okg.derived_artifact_runs(run_id),
  input_scope           TEXT NOT NULL,
  blocking_rule_hash    TEXT NOT NULL,
  participant_roles     JSONB NOT NULL,
  participant_anchors   JSONB NOT NULL,
  predicate_key         TEXT NOT NULL,
  generation_policy     TEXT NOT NULL,
  coverage_status       TEXT NOT NULL
    CHECK (coverage_status IN (
      'candidate', 'excluded', 'not_evaluated'
    )),
  exclusion_reason      TEXT,
  candidate_payload     JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (check_id, target_generation_id, candidate_id)
);

CREATE INDEX IF NOT EXISTS consistency_candidates_run_idx
  ON okg.consistency_candidates (run_id);

CREATE INDEX IF NOT EXISTS consistency_candidates_scope_idx
  ON okg.consistency_candidates
  (check_id, target_generation_id, input_scope);

CREATE TABLE IF NOT EXISTS okg.consistency_candidate_coverage (
  run_id                      UUID PRIMARY KEY
    REFERENCES okg.derived_artifact_runs(run_id),
  check_id                    TEXT NOT NULL
    REFERENCES okg.consistency_check_definitions(check_id),
  target_generation_id        BIGINT NOT NULL,
  universe_key                TEXT NOT NULL,
  coverage_basis              TEXT NOT NULL
    CHECK (coverage_basis IN (
      'exact_postgres',
      'exact_snapshot',
      'heuristic',
      'sampled_monitoring',
      'incomplete'
    )),
  coverage_complete           BOOLEAN NOT NULL,
  -- Scoped-basis first-class columns (task.scope.first-class-basis-and-health).
  -- `scoped` = the candidate universe was scope-restricted (an incremental
  -- subset of the check universe), read DIRECTLY by the gate algebra / doctor /
  -- operator surfaces -- not as a payload->>'scope_restricted' convention.
  -- `scope_subject_count` = how many subjects that scope covered. An
  -- engine-certified scoped-exact run (scoped + an exact coverage_basis) is
  -- HEALTHY though not universe-complete; the scoped-complete CHECK below
  -- enforces that a scoped run never claims coverage_complete.
  scoped                      BOOLEAN NOT NULL DEFAULT false,
  scope_subject_count         BIGINT NOT NULL DEFAULT 0
    CHECK (scope_subject_count >= 0),
  eligible_input_count        BIGINT NOT NULL DEFAULT 0
    CHECK (eligible_input_count >= 0),
  excluded_input_counts       JSONB NOT NULL DEFAULT '{}'::jsonb,
  missing_text_count          BIGINT NOT NULL DEFAULT 0
    CHECK (missing_text_count >= 0),
  missing_embedding_count     BIGINT NOT NULL DEFAULT 0
    CHECK (missing_embedding_count >= 0),
  block_count                 BIGINT NOT NULL DEFAULT 0
    CHECK (block_count >= 0),
  candidate_count             BIGINT NOT NULL DEFAULT 0
    CHECK (candidate_count >= 0),
  candidate_count_by_block    JSONB NOT NULL DEFAULT '{}'::jsonb,
  largest_block_size          BIGINT NOT NULL DEFAULT 0
    CHECK (largest_block_size >= 0),
  p95_block_size              NUMERIC(18, 4) NOT NULL DEFAULT 0
    CHECK (p95_block_size >= 0),
  all_pairs_upper_bound       BIGINT NOT NULL DEFAULT 0
    CHECK (all_pairs_upper_bound >= 0),
  reduction_ratio             NUMERIC(18, 10) NOT NULL DEFAULT 0
    CHECK (reduction_ratio >= 0),
  evaluator_invocation_count  BIGINT NOT NULL DEFAULT 0
    CHECK (evaluator_invocation_count >= 0),
  evaluator_cost              JSONB NOT NULL DEFAULT '{}'::jsonb,
  false_negative_audit        JSONB NOT NULL DEFAULT '{}'::jsonb,
  payload                     JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (
    NOT coverage_complete
    OR coverage_basis IN ('exact_postgres', 'exact_snapshot')
  ),
  CHECK (
    NOT coverage_complete
    OR (missing_text_count = 0 AND missing_embedding_count = 0)
  )
);

-- Scoped-basis columns migration (task.scope.first-class-basis-and-health) for
-- EXISTING deployments; idempotent, a no-op on a freshly-created table. The
-- named scoped-complete CHECK is managed here (not inline in CREATE TABLE) so it
-- has a single definition across fresh + migrated databases.
DO $consistency_candidate_coverage_scoped_columns$
BEGIN
  ALTER TABLE okg.consistency_candidate_coverage
    ADD COLUMN IF NOT EXISTS scoped BOOLEAN NOT NULL DEFAULT false;
  ALTER TABLE okg.consistency_candidate_coverage
    ADD COLUMN IF NOT EXISTS scope_subject_count BIGINT NOT NULL DEFAULT 0;
  -- A scope-restricted run can never be universe-complete (the write-boundary
  -- invariant record_candidate_coverage raises on, now also enforced at the DB).
  -- Existing rows predate the column (scoped defaults false), so this holds.
  ALTER TABLE okg.consistency_candidate_coverage
    DROP CONSTRAINT IF EXISTS consistency_candidate_coverage_scoped_complete_check;
  ALTER TABLE okg.consistency_candidate_coverage
    ADD CONSTRAINT consistency_candidate_coverage_scoped_complete_check
    CHECK (NOT (coverage_complete AND scoped));
END
$consistency_candidate_coverage_scoped_columns$;

CREATE INDEX IF NOT EXISTS consistency_candidate_coverage_check_idx
  ON okg.consistency_candidate_coverage
  (check_id, target_generation_id, coverage_complete);

CREATE TABLE IF NOT EXISTS okg.observation_events (
  event_id              BIGSERIAL PRIMARY KEY,
  observation_id        TEXT NOT NULL,
  check_id              TEXT NOT NULL
    REFERENCES okg.consistency_check_definitions(check_id),
  candidate_id          TEXT NOT NULL,
  target_generation_id  BIGINT NOT NULL,
  run_id                UUID NOT NULL
    REFERENCES okg.derived_artifact_runs(run_id),
  event_seq             INT NOT NULL,
  finding_event         TEXT
    CHECK (
      finding_event IS NULL
      OR finding_event IN ('observed', 'resolved', 'reopened')
    ),
  evaluation_event      TEXT
    CHECK (
      evaluation_event IS NULL
      OR evaluation_event IN (
        'evaluated', 'not_evaluated', 'coverage_incomplete', 'error'
      )
    ),
  input_scope           TEXT NOT NULL,
  predicate_key         TEXT NOT NULL,
  normalized_predicate  TEXT NOT NULL,
  normalized_values     JSONB NOT NULL DEFAULT '{}'::jsonb,
  participant_roles     JSONB NOT NULL,
  participant_anchors   JSONB NOT NULL,
  severity              TEXT,
  confidence            NUMERIC(8, 6),
  confidence_type       TEXT,
  evidence              JSONB NOT NULL DEFAULT '{}'::jsonb,
  payload               JSONB NOT NULL DEFAULT '{}'::jsonb,
  coverage_complete     BOOLEAN NOT NULL DEFAULT true,
  created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (run_id, event_seq),
  CHECK (
    (finding_event IS NOT NULL AND evaluation_event IS NULL)
    OR (finding_event IS NULL AND evaluation_event IS NOT NULL)
  )
);

CREATE INDEX IF NOT EXISTS observation_events_observation_idx
  ON okg.observation_events
  (observation_id, target_generation_id, event_seq);

CREATE INDEX IF NOT EXISTS observation_events_check_idx
  ON okg.observation_events
  (check_id, target_generation_id, candidate_id);

CREATE TABLE IF NOT EXISTS okg.observation_current (
  observation_id          TEXT PRIMARY KEY,
  check_id                TEXT NOT NULL
    REFERENCES okg.consistency_check_definitions(check_id),
  candidate_id            TEXT NOT NULL,
  latest_generation_id    BIGINT NOT NULL,
  latest_run_id           UUID NOT NULL
    REFERENCES okg.derived_artifact_runs(run_id),
  latest_event_id         BIGINT NOT NULL
    REFERENCES okg.observation_events(event_id),
  finding_status          TEXT NOT NULL
    CHECK (finding_status IN ('absent', 'open', 'resolved')),
  evaluation_status       TEXT NOT NULL
    CHECK (evaluation_status IN (
      'evaluated', 'not_evaluated', 'coverage_incomplete', 'error'
    )),
  input_scope             TEXT NOT NULL,
  predicate_key           TEXT NOT NULL,
  normalized_predicate    TEXT NOT NULL,
  normalized_values       JSONB NOT NULL DEFAULT '{}'::jsonb,
  participant_roles       JSONB NOT NULL,
  participant_anchors     JSONB NOT NULL,
  severity                TEXT,
  confidence              NUMERIC(8, 6),
  confidence_type         TEXT,
  evidence                JSONB NOT NULL DEFAULT '{}'::jsonb,
  payload                 JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS observation_current_check_status_idx
  ON okg.observation_current
  (check_id, finding_status, evaluation_status);

CREATE TABLE IF NOT EXISTS okg.consistency_gate_results (
  result_id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  run_id                 UUID NOT NULL
    REFERENCES okg.derived_artifact_runs(run_id),
  check_id               TEXT NOT NULL
    REFERENCES okg.consistency_check_definitions(check_id),
  target_generation_id   BIGINT NOT NULL,
  gate_name              TEXT NOT NULL,
  -- result is EVALUATION HEALTH only (consistency-blocking-policy
  -- task.policy.result-fields): 'evaluated' = the check ran cleanly, with OR
  -- without open findings; the 'blocked_*' values are operational health
  -- (coverage-incomplete / timeout / error). Finding presence lives in the
  -- *_count columns and the publish decision in blocks_publish -- NEVER in this
  -- column. Legacy 'pass'/'fail' both collapsed into 'evaluated' (the finding
  -- count is retained in open_observation_count, so the collapse is lossless).
  result                 TEXT NOT NULL
    CONSTRAINT consistency_gate_results_result_check CHECK (result IN (
      'evaluated', 'blocked_incomplete',
      'blocked_timeout', 'blocked_error'
    )),
  blocks_publish         BOOLEAN NOT NULL,
  reason                 TEXT NOT NULL,
  -- finding counts, first-class (task.policy.result-fields): open = findings
  -- open at/below the target generation; newly_opened = findings this run OPENED
  -- (a finding_event='observed' row for run_id); resolved = findings this run
  -- resolved (finding_event='resolved'). Operator surfaces and health metrics
  -- read these instead of inferring findings from the result string.
  open_observation_count BIGINT NOT NULL DEFAULT 0
    CHECK (open_observation_count >= 0),
  newly_opened_count     BIGINT NOT NULL DEFAULT 0
    CHECK (newly_opened_count >= 0),
  resolved_count         BIGINT NOT NULL DEFAULT 0
    CHECK (resolved_count >= 0),
  -- Backpressure PROBE marker (consistency-gate-durability
  -- task.backpressure.probe): true iff this row is a PROBE run -- the evaluator
  -- re-run a degraded gate performed instead of short-circuiting, to test
  -- whether the underlying failure has cleared. summarize_consistency_gate_health
  -- counts probes separately and drives the probe cadence off them, so a probe's
  -- own blocked_error re-enters short-circuit WITHOUT permanently latching the
  -- degraded state. Ordinary runs AND short-circuit rows are false.
  is_probe               BOOLEAN NOT NULL DEFAULT false,
  coverage_complete      BOOLEAN,
  coverage_basis         TEXT,
  budget_ms              BIGINT CHECK (budget_ms IS NULL OR budget_ms >= 0),
  elapsed_ms             BIGINT NOT NULL DEFAULT 0
    CHECK (elapsed_ms >= 0),
  preflight              JSONB NOT NULL DEFAULT '{}'::jsonb,
  details                JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (run_id, gate_name)
);

-- result-vocabulary split migration (consistency-blocking-policy
-- task.policy.result-fields) for EXISTING deployments. Idempotent; a no-op on a
-- freshly-created table. Order matters: add columns, drop the obsolete
-- pass=>not-blocks coupling, then migrate the data BEFORE swapping the domain
-- CHECK so the new constraint holds at ADD time.
DO $consistency_gate_results_result_vocabulary$
BEGIN
  ALTER TABLE okg.consistency_gate_results
    ADD COLUMN IF NOT EXISTS newly_opened_count BIGINT NOT NULL DEFAULT 0;
  ALTER TABLE okg.consistency_gate_results
    ADD COLUMN IF NOT EXISTS resolved_count BIGINT NOT NULL DEFAULT 0;
  -- Evaluation health is now decoupled from the publish decision, so a healthy
  -- (evaluated) run with open findings under an any_open policy legitimately
  -- blocks. The old "a pass never blocks" invariant no longer holds -- drop it
  -- (both the original table-level auto-name and the later explicit name).
  ALTER TABLE okg.consistency_gate_results
    DROP CONSTRAINT IF EXISTS consistency_gate_results_blocks_publish_check;
  ALTER TABLE okg.consistency_gate_results
    DROP CONSTRAINT IF EXISTS consistency_gate_results_check;
  -- Swap the result domain: 'pass'/'fail' -> 'evaluated' (lossless, the finding
  -- count survives in open_observation_count). Drop the old domain CHECK first,
  -- migrate the rows, then re-add the domain with the new vocabulary.
  ALTER TABLE okg.consistency_gate_results
    DROP CONSTRAINT IF EXISTS consistency_gate_results_result_check;
  UPDATE okg.consistency_gate_results
     SET result = 'evaluated'
   WHERE result IN ('pass', 'fail');
  ALTER TABLE okg.consistency_gate_results
    ADD CONSTRAINT consistency_gate_results_result_check
    CHECK (result IN (
      'evaluated', 'blocked_incomplete', 'blocked_timeout', 'blocked_error'
    ));
END
$consistency_gate_results_result_vocabulary$;

-- Backpressure probe marker for existing deployments (consistency-gate-durability
-- task.backpressure.probe). Idempotent additive column; a no-op on a freshly
-- created table. Existing rows default to false (ordinary / short-circuit runs).
ALTER TABLE okg.consistency_gate_results
  ADD COLUMN IF NOT EXISTS is_probe BOOLEAN NOT NULL DEFAULT false;

CREATE INDEX IF NOT EXISTS consistency_gate_results_check_idx
  ON okg.consistency_gate_results
  (check_id, gate_name, target_generation_id);

CREATE INDEX IF NOT EXISTS consistency_gate_results_blocked_idx
  ON okg.consistency_gate_results
  (check_id, gate_name, created_at DESC)
  WHERE blocks_publish;

CREATE TABLE IF NOT EXISTS okg.lens_definitions (
  definition_id             TEXT PRIMARY KEY
    REFERENCES okg.derived_artifact_definitions(definition_id),
  deployment_name           TEXT NOT NULL,
  lens_id                   TEXT NOT NULL,
  view_definition_id        TEXT NOT NULL
    REFERENCES okg.derived_artifact_definitions(definition_id),
  view_id                   TEXT NOT NULL,
  lens_contract             JSONB NOT NULL DEFAULT '{}'::jsonb,
  putback_policy            JSONB NOT NULL DEFAULT '{}'::jsonb,
  source_authority_policy   JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (deployment_name, lens_id)
);

-- AKMON Phase 5 (add-lens-promotion-admission): lens definitions
-- can opt into overlay-to-base promotion by declaring a mapping
-- from overlay-delta keys (slot_name, scope_key, row_key,
-- field_path) to graph mutation keys (node_id, subtype,
-- attrs.<field>). NULL means promotion is not supported for this
-- lens; overlay rows remain in `committed_overlay` indefinitely.
ALTER TABLE okg.lens_definitions
  ADD COLUMN IF NOT EXISTS lens_promotion_mapping JSONB;

CREATE INDEX IF NOT EXISTS lens_definitions_view_idx
  ON okg.lens_definitions
  (deployment_name, view_definition_id);

CREATE TABLE IF NOT EXISTS okg.lens_putback_requests (
  request_id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  lens_definition_id       TEXT NOT NULL
    REFERENCES okg.lens_definitions(definition_id),
  deployment_name          TEXT NOT NULL,
  lens_id                  TEXT NOT NULL,
  base_view_definition_id  TEXT NOT NULL
    REFERENCES okg.derived_artifact_definitions(definition_id),
  base_view_manifest_id    UUID NOT NULL
    REFERENCES okg.derived_output_manifests(manifest_id),
  target_generation_id     BIGINT NOT NULL,
  actor_id                 TEXT NOT NULL,
  idempotency_key          TEXT,
  status                   TEXT NOT NULL
    CHECK (status IN (
      'preparing', 'prepared', 'conflict', 'committed_overlay',
      'rejected', 'failed', 'promoted'
    )),
  commit_mode              TEXT NOT NULL DEFAULT 'overlay'
    CHECK (commit_mode IN ('overlay')),
  delta_count              BIGINT NOT NULL DEFAULT 0
    CHECK (delta_count >= 0),
  conflict_count           BIGINT NOT NULL DEFAULT 0
    CHECK (conflict_count >= 0),
  error                    JSONB,
  metadata                 JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
  prepared_at              TIMESTAMPTZ,
  committed_at             TIMESTAMPTZ,
  rejected_at              TIMESTAMPTZ,
  UNIQUE (lens_definition_id, actor_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS lens_putback_requests_lens_idx
  ON okg.lens_putback_requests
  (lens_definition_id, target_generation_id, status, created_at DESC);

CREATE TABLE IF NOT EXISTS okg.lens_putback_deltas (
  delta_id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  request_id                UUID NOT NULL
    REFERENCES okg.lens_putback_requests(request_id) ON DELETE CASCADE,
  delta_index               INT NOT NULL CHECK (delta_index >= 0),
  slot_name                 TEXT NOT NULL,
  scope_key                 TEXT NOT NULL,
  row_key                   TEXT NOT NULL,
  field_path                TEXT[] NOT NULL,
  expected_value            JSONB,
  new_value                 JSONB NOT NULL,
  source_authority          TEXT NOT NULL,
  source_record_id          JSONB NOT NULL,
  expected_source_revision  JSONB,
  source_delta              JSONB NOT NULL DEFAULT '{}'::jsonb,
  validation_status         TEXT NOT NULL
    CHECK (validation_status IN ('prepared', 'conflict')),
  validation_error          JSONB,
  metadata                  JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (request_id, delta_index)
);

CREATE INDEX IF NOT EXISTS lens_putback_deltas_request_idx
  ON okg.lens_putback_deltas
  (request_id, validation_status);

CREATE TABLE IF NOT EXISTS okg.lens_overlay_log (
  overlay_id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  request_id                UUID NOT NULL
    REFERENCES okg.lens_putback_requests(request_id),
  delta_id                  UUID NOT NULL
    REFERENCES okg.lens_putback_deltas(delta_id),
  lens_definition_id        TEXT NOT NULL
    REFERENCES okg.lens_definitions(definition_id),
  target_generation_id      BIGINT NOT NULL,
  source_authority          TEXT NOT NULL,
  source_record_id          JSONB NOT NULL,
  expected_source_revision  JSONB,
  field_path                TEXT[] NOT NULL,
  old_value                 JSONB,
  new_value                 JSONB NOT NULL,
  source_delta              JSONB NOT NULL DEFAULT '{}'::jsonb,
  status                    TEXT NOT NULL
    CHECK (status IN (
      'active', 'superseded', 'promoted', 'superseded_by_promotion'
    )),
  promoted_admission_id     UUID
    REFERENCES okg.admission_envelopes(admission_id),
  metadata                  JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
  superseded_at             TIMESTAMPTZ,
  promoted_at               TIMESTAMPTZ,
  UNIQUE (delta_id)
);

CREATE INDEX IF NOT EXISTS lens_overlay_log_lens_idx
  ON okg.lens_overlay_log
  (lens_definition_id, target_generation_id, status, created_at DESC);

CREATE INDEX IF NOT EXISTS lens_overlay_log_row_anchor_idx
  ON okg.lens_overlay_log
  (
    lens_definition_id,
    target_generation_id,
    status,
    ((source_delta->>'slot_name')),
    ((source_delta->>'scope_key')),
    ((source_delta->>'row_key'))
  );

-- AKMON Phase 5 (add-lens-promotion-admission): widen
-- `lens_putback_requests.status` to include `'promoted'`, widen
-- `lens_overlay_log.status` to include `'promoted'` and
-- `'superseded_by_promotion'`, and add `promoted_admission_id` /
-- `promoted_at` to the overlay log. Idempotent so existing DBs pick
-- up the new vocabulary.
DO $$
BEGIN
  ALTER TABLE okg.lens_putback_requests
    DROP CONSTRAINT IF EXISTS lens_putback_requests_status_check;
  ALTER TABLE okg.lens_putback_requests
    ADD CONSTRAINT lens_putback_requests_status_check
    CHECK (status IN (
      'preparing', 'prepared', 'conflict', 'committed_overlay',
      'rejected', 'failed', 'promoted'
    ));

  ALTER TABLE okg.lens_overlay_log
    DROP CONSTRAINT IF EXISTS lens_overlay_log_status_check;
  ALTER TABLE okg.lens_overlay_log
    ADD CONSTRAINT lens_overlay_log_status_check
    CHECK (status IN (
      'active', 'superseded', 'promoted', 'superseded_by_promotion'
    ));
END
$$;

ALTER TABLE okg.lens_overlay_log
  ADD COLUMN IF NOT EXISTS promoted_admission_id UUID
    REFERENCES okg.admission_envelopes(admission_id);

ALTER TABLE okg.lens_overlay_log
  ADD COLUMN IF NOT EXISTS promoted_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS lens_overlay_log_promotion_idx
  ON okg.lens_overlay_log (promoted_admission_id)
  WHERE promoted_admission_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS okg.derived_artifact_dependencies (
  dependency_id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  downstream_definition_id   TEXT NOT NULL
    REFERENCES okg.derived_artifact_definitions(definition_id),
  upstream_definition_id     TEXT NOT NULL
    REFERENCES okg.derived_artifact_definitions(definition_id),
  upstream_slot_name         TEXT NOT NULL DEFAULT 'relation',
  dependency_kind            TEXT NOT NULL
    CHECK (dependency_kind IN ('manifest_slot')),
  scope_key                  TEXT,
  required_compute_hash      TEXT,
  required_output_schema_hash TEXT,
  required_renderer_hash     TEXT,
  required_dirty_rule_hash   TEXT,
  metadata                   JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at                 TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (downstream_definition_id <> upstream_definition_id)
);

CREATE INDEX IF NOT EXISTS derived_artifact_dependencies_downstream_idx
  ON okg.derived_artifact_dependencies
  (downstream_definition_id, upstream_definition_id);

CREATE UNIQUE INDEX IF NOT EXISTS derived_artifact_dependencies_uidx
  ON okg.derived_artifact_dependencies
  (downstream_definition_id, upstream_definition_id,
   upstream_slot_name, (COALESCE(scope_key, '')));

CREATE TABLE IF NOT EXISTS okg.derived_schedule_plans (
  plan_id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  deployment_name      TEXT NOT NULL,
  target_generation_id BIGINT NOT NULL,
  execution_phase      TEXT NOT NULL
    CHECK (execution_phase IN ('post_publish', 'pre_publish_gate')),
  catalog_version_id   BIGINT,
  graph_hash           TEXT NOT NULL,
  status               TEXT NOT NULL
    CHECK (status IN ('running', 'completed', 'failed')),
  error                JSONB,
  started_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  completed_at         TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS derived_schedule_plans_target_idx
  ON okg.derived_schedule_plans
  (deployment_name, target_generation_id, execution_phase, status);

CREATE TABLE IF NOT EXISTS okg.derived_schedule_nodes (
  plan_id              UUID NOT NULL
    REFERENCES okg.derived_schedule_plans(plan_id) ON DELETE CASCADE,
  definition_id        TEXT NOT NULL
    REFERENCES okg.derived_artifact_definitions(definition_id),
  topo_index           INT NOT NULL,
  status               TEXT NOT NULL
    CHECK (status IN (
      'planned', 'running', 'completed', 'carried_forward', 'failed'
    )),
  run_id               UUID
    REFERENCES okg.derived_artifact_runs(run_id),
  dirty_scope_count    BIGINT NOT NULL DEFAULT 0
    CHECK (dirty_scope_count >= 0),
  upstream_binding_count BIGINT NOT NULL DEFAULT 0
    CHECK (upstream_binding_count >= 0),
  error                JSONB,
  started_at           TIMESTAMPTZ,
  completed_at         TIMESTAMPTZ,
  PRIMARY KEY (plan_id, definition_id),
  UNIQUE (plan_id, topo_index)
);

CREATE INDEX IF NOT EXISTS derived_schedule_nodes_definition_idx
  ON okg.derived_schedule_nodes (definition_id, status);

CREATE TABLE IF NOT EXISTS okg.derived_run_dependency_bindings (
  binding_id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  run_id                       UUID NOT NULL
    REFERENCES okg.derived_artifact_runs(run_id) ON DELETE CASCADE,
  downstream_definition_id     TEXT NOT NULL
    REFERENCES okg.derived_artifact_definitions(definition_id),
  upstream_definition_id       TEXT NOT NULL
    REFERENCES okg.derived_artifact_definitions(definition_id),
  upstream_manifest_id         UUID NOT NULL
    REFERENCES okg.derived_output_manifests(manifest_id),
  upstream_slot_name           TEXT NOT NULL,
  upstream_target_generation_id BIGINT NOT NULL,
  scope_key                    TEXT,
  required_compute_hash        TEXT,
  required_output_schema_hash  TEXT,
  required_renderer_hash       TEXT,
  required_dirty_rule_hash     TEXT,
  bound_compute_hash           TEXT NOT NULL,
  bound_output_schema_hash     TEXT NOT NULL,
  bound_renderer_hash          TEXT NOT NULL,
  bound_dirty_rule_hash        TEXT NOT NULL,
  metadata                     JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at                   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS derived_run_dependency_bindings_run_idx
  ON okg.derived_run_dependency_bindings (run_id);

CREATE UNIQUE INDEX IF NOT EXISTS derived_run_dependency_bindings_uidx
  ON okg.derived_run_dependency_bindings
  (run_id, upstream_definition_id, upstream_slot_name,
   (COALESCE(scope_key, '')));

CREATE INDEX IF NOT EXISTS derived_run_dependency_bindings_upstream_idx
  ON okg.derived_run_dependency_bindings
  (upstream_definition_id, upstream_target_generation_id);

-- Chronos-native generations are checkpoint ids stored in Chronos
-- metadata. Control-plane rows may retain an integer
-- target_generation_id for grouping and ordering, but it is no longer
-- a foreign key to okg.graph_generations.
DO $chronos_native_drop_generation_fks$
DECLARE
  rec RECORD;
BEGIN
  FOR rec IN
    SELECT conrelid::regclass AS relname, conname
      FROM pg_constraint
     WHERE contype = 'f'
       AND confrelid = 'okg.graph_generations'::regclass
  LOOP
    EXECUTE format(
      'ALTER TABLE %s DROP CONSTRAINT IF EXISTS %I',
      rec.relname,
      rec.conname
    );
  END LOOP;
END
$chronos_native_drop_generation_fks$;

-- ========== Retired legacy runtime audit ==========
--
-- The physical `okg.publisher_leases` and `okg.jobs` runtime tables are
-- retired. Existing databases remove both job-runtime generations through the
-- explicit `okg db remove-job-runtime-subsystem --confirm --apply` command and
-- migrations/0002_remove_job_runtime_subsystem.sql. Fresh schema does not
-- recreate them.

CREATE TABLE IF NOT EXISTS okg.lease_audit (
  audit_id          BIGSERIAL PRIMARY KEY,
  at                TIMESTAMPTZ NOT NULL DEFAULT now(),
  deployment_name   TEXT        NOT NULL,
  scope             TEXT        NOT NULL,
  event             TEXT        NOT NULL
                                CHECK (event IN (
                                  'acquired','released',
                                  'stolen','lost','force_released'
                                )),
  holder_id         TEXT        NOT NULL,
  prior_holder_id   TEXT,
  lease_seq         BIGINT      NOT NULL,
  holder_meta       JSONB       NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS lease_audit_recent_idx
  ON okg.lease_audit (at DESC, deployment_name);

-- ========== Source watermarks ==========

CREATE TABLE IF NOT EXISTS okg.source_watermarks (
  source              TEXT       PRIMARY KEY,
  cutover_watermark   JSONB      NOT NULL DEFAULT '{}'::jsonb,
  last_applied_rev    JSONB      NOT NULL DEFAULT '{}'::jsonb,
  last_published_rev  JSONB      NOT NULL DEFAULT '{}'::jsonb,
  projection_cursor   BIGINT     NOT NULL DEFAULT 0,
  last_reconcile_at   TIMESTAMPTZ,
  replay_required     JSONB      NOT NULL DEFAULT '{}'::jsonb,
  replay_required_at  TIMESTAMPTZ
);

ALTER TABLE okg.source_watermarks
  ADD COLUMN IF NOT EXISTS replay_required JSONB NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS replay_required_at TIMESTAMPTZ;

-- Large record-set authority lives off the hot watermark row. The
-- watermark stores a compact record_set_ref plus record_set_summary; the
-- runner loads this side table only when it must compute record-set
-- retractions or coverage. This keeps scoped source-sync deltas from
-- rewriting multi-megabyte JSONB blobs on every event.
CREATE TABLE IF NOT EXISTS okg.source_record_sets (
  source              TEXT       PRIMARY KEY,
  record_set          JSONB      NOT NULL,
  record_set_summary  JSONB      NOT NULL DEFAULT '{}'::jsonb,
  chronos_proof       JSONB,
  sha256              TEXT       NOT NULL DEFAULT '',
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS okg.source_record_set_blobs (
  source              TEXT       NOT NULL,
  sha256              TEXT       NOT NULL,
  record_set          JSONB      NOT NULL,
  record_set_text     TEXT,
  record_set_summary  JSONB      NOT NULL DEFAULT '{}'::jsonb,
  chronos_proof       JSONB,
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (source, sha256)
);

-- Pieced record-set authority (issue #3008). A record set above the JSONB
-- threshold is stored as ordered pieces under its header row; the header is
-- written last in the same transaction and a deferred foreign key refuses a
-- piece without a header. Same definitions as migration 0035 and the source
-- runner's runtime bootstrap (ingest/record_set_pieces.py).
ALTER TABLE okg.source_record_set_blobs
  ADD COLUMN IF NOT EXISTS piece_write_id UUID,
  ADD COLUMN IF NOT EXISTS piece_count INTEGER,
  ADD COLUMN IF NOT EXISTS piece_bytes BIGINT;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conrelid = 'okg.source_record_set_blobs'::regclass
       AND conname = 'source_record_set_blobs_piece_write_id_key'
  ) THEN
    ALTER TABLE okg.source_record_set_blobs
      ADD CONSTRAINT source_record_set_blobs_piece_write_id_key
      UNIQUE (piece_write_id);
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conrelid = 'okg.source_record_set_blobs'::regclass
       AND conname = 'source_record_set_blobs_pieces_shape'
  ) THEN
    ALTER TABLE okg.source_record_set_blobs
      ADD CONSTRAINT source_record_set_blobs_pieces_shape CHECK (
        (piece_write_id IS NULL AND piece_count IS NULL
          AND piece_bytes IS NULL)
        OR (piece_write_id IS NOT NULL AND piece_count > 0
          AND piece_bytes >= 0 AND record_set_text IS NULL)
      );
  END IF;
END
$$;

CREATE TABLE IF NOT EXISTS okg.source_record_set_blob_pieces (
  write_id  UUID    NOT NULL,
  piece_no  INTEGER NOT NULL CHECK (piece_no >= 0),
  source    TEXT    NOT NULL,
  piece     TEXT    NOT NULL,
  PRIMARY KEY (write_id, piece_no),
  CONSTRAINT source_record_set_blob_pieces_header_fkey
    FOREIGN KEY (write_id)
    REFERENCES okg.source_record_set_blobs (piece_write_id)
    ON DELETE CASCADE
    DEFERRABLE INITIALLY DEFERRED
);

-- Indexed record authority v2 is opt-in. Legacy record-set tables remain the
-- default and are not rewritten. Staging rows are private; only a sealed row
-- bound through source_record_authority_refs is public authority.

CREATE TABLE IF NOT EXISTS okg.source_record_authorities (
  authority_id UUID PRIMARY KEY,
  source TEXT NOT NULL CHECK (btrim(source) <> ''),
  incarnation TEXT NOT NULL CHECK (btrim(incarnation) <> ''),
  attempt TEXT NOT NULL CHECK (btrim(attempt) <> ''),
  format TEXT NOT NULL DEFAULT 'okg.source_runner.record_authority.indexed.v2'
    CHECK (format = 'okg.source_runner.record_authority.indexed.v2'),
  state TEXT NOT NULL DEFAULT 'staging'
    CHECK (state IN ('staging', 'sealed')),
  logical_sha256 TEXT,
  record_count BIGINT,
  operation_count BIGINT,
  component_count BIGINT,
  logical_bytes BIGINT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  sealed_at TIMESTAMPTZ,
  UNIQUE (source, incarnation, attempt),
  UNIQUE (
    authority_id, source, incarnation, logical_sha256, record_count, state
  ),
  CHECK (
    (state = 'staging' AND sealed_at IS NULL)
    OR (
      state = 'sealed'
      AND sealed_at IS NOT NULL
      AND logical_sha256 IS NOT NULL
      AND logical_sha256 ~ '^[0-9a-f]{64}$'
      AND record_count IS NOT NULL
      AND record_count >= 0
      AND operation_count IS NOT NULL
      AND operation_count >= 0
      AND component_count IS NOT NULL
      AND component_count >= 0
      AND logical_bytes IS NOT NULL
      AND logical_bytes >= 0
    )
  )
);

CREATE TABLE IF NOT EXISTS okg.source_record_authority_payloads (
  sha256 TEXT PRIMARY KEY CHECK (sha256 ~ '^[0-9a-f]{64}$'),
  payload_kind TEXT NOT NULL CHECK (payload_kind = 'json'),
  byte_count BIGINT NOT NULL CHECK (byte_count >= 0),
  chunk_count BIGINT NOT NULL CHECK (chunk_count > 0)
);

CREATE TABLE IF NOT EXISTS okg.source_record_authority_payload_chunks (
  sha256 TEXT NOT NULL
    REFERENCES okg.source_record_authority_payloads(sha256),
  chunk_ordinal BIGINT NOT NULL CHECK (chunk_ordinal >= 0),
  payload BYTEA NOT NULL CHECK (octet_length(payload) <= 1048576),
  PRIMARY KEY (sha256, chunk_ordinal)
);

CREATE TABLE IF NOT EXISTS okg.source_record_authority_operations (
  authority_id UUID NOT NULL
    REFERENCES okg.source_record_authorities(authority_id),
  ordinal BIGINT NOT NULL CHECK (ordinal >= 0),
  record_id TEXT COLLATE "C" NOT NULL CHECK (btrim(record_id) <> ''),
  operation TEXT NOT NULL CHECK (operation IN ('upsert', 'retract')),
  operation_sha256 TEXT NOT NULL
    CHECK (operation_sha256 ~ '^[0-9a-f]{64}$'),
  component_count BIGINT NOT NULL CHECK (component_count >= 0),
  PRIMARY KEY (authority_id, ordinal)
);

CREATE TABLE IF NOT EXISTS okg.source_record_authority_records (
  authority_id UUID NOT NULL,
  record_id TEXT COLLATE "C" NOT NULL,
  operation_ordinal BIGINT NOT NULL,
  PRIMARY KEY (authority_id, record_id),
  FOREIGN KEY (authority_id, operation_ordinal)
    REFERENCES okg.source_record_authority_operations(authority_id, ordinal)
);

CREATE INDEX IF NOT EXISTS source_record_authority_records_ordinal_idx
  ON okg.source_record_authority_records(authority_id, operation_ordinal);

CREATE TABLE IF NOT EXISTS okg.source_record_authority_components (
  authority_id UUID NOT NULL,
  operation_ordinal BIGINT NOT NULL,
  component_kind TEXT NOT NULL
    CHECK (component_kind IN ('primary', 'descendant', 'edge')),
  component_ordinal BIGINT NOT NULL CHECK (component_ordinal >= 0),
  PRIMARY KEY (
    authority_id, operation_ordinal, component_kind, component_ordinal
  ),
  FOREIGN KEY (authority_id, operation_ordinal)
    REFERENCES okg.source_record_authority_operations(authority_id, ordinal),
  CHECK (
    (component_kind = 'primary' AND component_ordinal = 0)
    OR component_kind <> 'primary'
  )
);

CREATE TABLE IF NOT EXISTS okg.source_record_authority_component_fields (
  authority_id UUID NOT NULL,
  operation_ordinal BIGINT NOT NULL,
  component_kind TEXT NOT NULL,
  component_ordinal BIGINT NOT NULL,
  field_name TEXT COLLATE "C" NOT NULL CHECK (field_name <> ''),
  payload_sha256 TEXT NOT NULL
    REFERENCES okg.source_record_authority_payloads(sha256),
  PRIMARY KEY (
    authority_id, operation_ordinal, component_kind, component_ordinal,
    field_name
  ),
  FOREIGN KEY (
    authority_id, operation_ordinal, component_kind, component_ordinal
  ) REFERENCES okg.source_record_authority_components(
    authority_id, operation_ordinal, component_kind, component_ordinal
  )
);

CREATE TABLE IF NOT EXISTS okg.source_record_authority_identities (
  authority_id UUID NOT NULL,
  operation_ordinal BIGINT NOT NULL,
  identity_kind TEXT NOT NULL CHECK (identity_kind IN ('node', 'edge')),
  identity TEXT COLLATE "C" NOT NULL,
  ownership TEXT NOT NULL,
  PRIMARY KEY (
    authority_id, operation_ordinal, identity_kind, identity, ownership
  ),
  FOREIGN KEY (authority_id, operation_ordinal)
    REFERENCES okg.source_record_authority_operations(authority_id, ordinal)
);

CREATE INDEX IF NOT EXISTS source_record_authority_identity_lookup_idx
  ON okg.source_record_authority_identities(
    authority_id, identity_kind, identity
  );

CREATE TABLE IF NOT EXISTS okg.source_record_authority_refs (
  source TEXT PRIMARY KEY,
  authority_id UUID NOT NULL UNIQUE,
  incarnation TEXT NOT NULL,
  logical_sha256 TEXT NOT NULL,
  record_count BIGINT NOT NULL CHECK (record_count >= 0),
  authority_state TEXT NOT NULL DEFAULT 'sealed'
    CHECK (authority_state = 'sealed'),
  bound_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  FOREIGN KEY (
    authority_id, source, incarnation, logical_sha256, record_count,
    authority_state
  ) REFERENCES okg.source_record_authorities(
    authority_id, source, incarnation, logical_sha256, record_count, state
  )
);

-- Transaction-private review handoff; never interpreted as a public record-set
-- reference. Decision processing reconstructs and revalidates the candidate.
CREATE TABLE IF NOT EXISTS okg.source_record_authority_pending_admissions (
  admission_id UUID PRIMARY KEY,
  authority_id UUID NOT NULL UNIQUE
    REFERENCES okg.source_record_authorities(authority_id),
  source TEXT NOT NULL,
  incarnation TEXT NOT NULL,
  attempt TEXT NOT NULL,
  expected_sha256 TEXT NOT NULL CHECK (expected_sha256 ~ '^[0-9a-f]{64}$'),
  expected_records BIGINT NOT NULL CHECK (expected_records >= 0),
  expected_operations BIGINT NOT NULL CHECK (expected_operations >= 0),
  expected_components BIGINT NOT NULL CHECK (expected_components >= 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION okg.reject_authority_payload_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'content-addressed authority payloads are immutable';
END;
$$;

CREATE OR REPLACE FUNCTION okg.guard_authority_header_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'DELETE' OR OLD.state <> 'staging' OR NEW.state <> 'sealed'
     OR (OLD.authority_id, OLD.source, OLD.incarnation, OLD.attempt, OLD.format)
        IS DISTINCT FROM
        (NEW.authority_id, NEW.source, NEW.incarnation, NEW.attempt, NEW.format)
  THEN
    RAISE EXCEPTION 'authority headers allow only one staging-to-sealed transition';
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS source_record_authorities_seal_once
  ON okg.source_record_authorities;
CREATE TRIGGER source_record_authorities_seal_once
BEFORE UPDATE OR DELETE ON okg.source_record_authorities
FOR EACH ROW EXECUTE FUNCTION okg.guard_authority_header_mutation();

DROP TRIGGER IF EXISTS source_record_authority_payloads_immutable
  ON okg.source_record_authority_payloads;
CREATE TRIGGER source_record_authority_payloads_immutable
BEFORE UPDATE OR DELETE ON okg.source_record_authority_payloads
FOR EACH ROW EXECUTE FUNCTION okg.reject_authority_payload_mutation();

DROP TRIGGER IF EXISTS source_record_authority_chunks_immutable
  ON okg.source_record_authority_payload_chunks;
CREATE TRIGGER source_record_authority_chunks_immutable
BEFORE UPDATE OR DELETE ON okg.source_record_authority_payload_chunks
FOR EACH ROW EXECUTE FUNCTION okg.reject_authority_payload_mutation();

CREATE OR REPLACE FUNCTION okg.guard_authority_child_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  target UUID := CASE WHEN TG_OP = 'DELETE' THEN OLD.authority_id
                      ELSE NEW.authority_id END;
  authority_state TEXT;
BEGIN
  IF TG_OP = 'UPDATE' AND OLD.authority_id IS DISTINCT FROM NEW.authority_id THEN
    RAISE EXCEPTION 'authority child rows cannot change authority_id';
  END IF;
  SELECT state INTO authority_state
    FROM okg.source_record_authorities
   WHERE authority_id = target FOR UPDATE;
  IF authority_state IS DISTINCT FROM 'staging' THEN
    RAISE EXCEPTION 'sealed or missing authority is immutable';
  END IF;
  RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
END;
$$;

DO $$
DECLARE table_name TEXT;
BEGIN
  FOREACH table_name IN ARRAY ARRAY[
    'source_record_authority_operations',
    'source_record_authority_records',
    'source_record_authority_components',
    'source_record_authority_component_fields',
    'source_record_authority_identities'
  ] LOOP
    EXECUTE format('DROP TRIGGER IF EXISTS %I ON okg.%I',
                   table_name || '_staging_only', table_name);
    EXECUTE format(
      'CREATE TRIGGER %I BEFORE INSERT OR UPDATE OR DELETE ON okg.%I '
      'FOR EACH ROW EXECUTE FUNCTION okg.guard_authority_child_mutation()',
      table_name || '_staging_only', table_name
    );
  END LOOP;
END;
$$;


-- Large top-level cursor fields also live off the hot watermark row.
-- Adapters still receive hydrated cursors; the watermark keeps compact
-- refs so rollback snapshots do not copy multi-megabyte JSONB fields on
-- every no-change run.
CREATE TABLE IF NOT EXISTS okg.source_cursor_blobs (
  source              TEXT       NOT NULL,
  cursor_key          TEXT       NOT NULL,
  sha256              TEXT       NOT NULL,
  value               JSONB      NOT NULL,
  value_text          TEXT,
  size_bytes          BIGINT     NOT NULL,
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (source, cursor_key, sha256)
);

-- Content-addressed figure-crop bytes staged between figure DISCOVERY
-- (DocCorpusSource, Phase A) and CAPTIONING (VisualCaptionEnricher, Phase B),
-- so the enricher never re-opens the PDF. Keyed by sha256(bytes) — the same
-- content-addressing shape as source_cursor_blobs. Declared here (not only
-- created on the fly by crop_store.ensure_table) so the table is part of the
-- canonical schema: visible to schema checks / backup tooling, and covered by
-- the grant below when reader and writer run as different database roles.
-- GC of superseded crops is tracked in the automate-source-blob-gc family.
CREATE TABLE IF NOT EXISTS okg.visual_crop_blobs (
  sha256              TEXT       PRIMARY KEY,
  bytes               BYTEA      NOT NULL,
  size_bytes          BIGINT     NOT NULL,
  mime_type           TEXT       NOT NULL DEFAULT 'image/png',
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Per-(visual, signature) caption-failure counter. The VisualCaptionEnricher
-- bounds VLM retry cost: after N VLM-call failures a figure is QUARANTINED (no
-- more VLM calls, watermark released), instead of re-billing a call every
-- publish forever. Rows are the loud, queryable record of quarantined figures;
-- a successful caption or a signature change clears/resets the budget. Same
-- content-addressed staging shape as visual_crop_blobs above.
CREATE TABLE IF NOT EXISTS okg.visual_caption_failures (
  visual_id           TEXT       NOT NULL,
  vision_signature    TEXT       NOT NULL,
  fail_count          INT        NOT NULL DEFAULT 0,
  last_error          TEXT,
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (visual_id, vision_signature)
);

-- Per-item/signature retry ledger for model-backed extraction enrichers.
-- Status is always explicit: there is deliberately no semantic default.
-- Claims, terminal quarantine, snapshot replacement, and enricher_progress
-- share the publisher connection and commit boundary.
CREATE TABLE IF NOT EXISTS okg.enricher_input_quarantines (
  deployment_name    TEXT        NOT NULL,
  branch_name        TEXT        NOT NULL,
  enricher_name      TEXT        NOT NULL,
  item_ref           TEXT        NOT NULL,
  input_signature    TEXT        NOT NULL,
  operator_version   TEXT        NOT NULL,
  model_id           TEXT        NOT NULL,
  prompt_hash        TEXT        NOT NULL,
  input_hash         TEXT        NOT NULL,
  status             TEXT        NOT NULL,
  attempt_count      INTEGER     NOT NULL,
  attempt_limit      INTEGER     NOT NULL,
  last_failure_class TEXT,
  last_error         TEXT,
  first_failed_at    TIMESTAMPTZ,
  last_failed_at     TIMESTAMPTZ,
  quarantined_at     TIMESTAMPTZ,
  resolved_at        TIMESTAMPTZ,
  superseded_at      TIMESTAMPTZ,
  claim_token        UUID,
  claimed_at         TIMESTAMPTZ,
  last_generation_id TEXT,
  created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (
    deployment_name, branch_name, enricher_name, item_ref, input_signature
  ),
  CONSTRAINT enricher_input_quarantine_status_chk
    CHECK (status IN ('retrying', 'active', 'superseded', 'resolved')),
  CONSTRAINT enricher_input_quarantine_attempt_count_chk
    CHECK (attempt_count >= 0 AND attempt_count <= attempt_limit),
  CONSTRAINT enricher_input_quarantine_attempt_limit_chk
    CHECK (attempt_limit > 0),
  CONSTRAINT enricher_input_quarantine_active_chk
    CHECK (
      status <> 'active'
      OR (attempt_count = attempt_limit AND quarantined_at IS NOT NULL)
    )
);
CREATE INDEX IF NOT EXISTS enricher_input_quarantine_active_idx
  ON okg.enricher_input_quarantines
    (deployment_name, status, branch_name, enricher_name, item_ref)
  WHERE status = 'active';

-- General blob store (PACT first-class-images, issue #3046). One row per
-- opaque id; `kind` is a closed set that grows by migration. For `image`
-- the id is the render id, and the table is both the byte store and the
-- render cache: a failure is stored under the same id with no bytes and a
-- named reason, so an unrenderable file is not retried every publish.
-- `last_used_at` is refreshed by every cache hit before the caller writes a
-- reference; cleanup (`okg db prune-blobs`) deletes per kind only past a
-- grace window on it. Migration 0041_blob_store carries the same DDL.
CREATE TABLE IF NOT EXISTS okg.blobs (
  id            TEXT        PRIMARY KEY,
  kind          TEXT        NOT NULL,
  bytes         BYTEA,
  sha256        TEXT,
  mime_type     TEXT,
  size_bytes    BIGINT,
  error         TEXT,
  meta          JSONB       NOT NULL DEFAULT '{}'::jsonb,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_used_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT blobs_kind_known CHECK (kind IN ('image')),
  -- Exactly one of bytes and error; the byte facts exist only with bytes.
  CONSTRAINT blobs_payload_shape CHECK (
    (bytes IS NOT NULL AND error IS NULL AND sha256 IS NOT NULL
      AND mime_type IS NOT NULL AND size_bytes IS NOT NULL)
    OR (bytes IS NULL AND error IS NOT NULL AND sha256 IS NULL
      AND mime_type IS NULL AND size_bytes IS NULL)
  )
);
CREATE INDEX IF NOT EXISTS blobs_kind_last_used_idx
  ON okg.blobs (kind, last_used_at);

-- The image rows only. okg_mcp reads images through this view (a grant
-- registry row in views.sql) and holds nothing on okg.blobs, so a later
-- kind never widens what that grant reaches. security_barrier keeps a
-- caller's own predicate from being evaluated over rows of another kind.
CREATE OR REPLACE VIEW okg.image_blobs WITH (security_barrier = true) AS
SELECT id, bytes, sha256, mime_type, size_bytes, error, meta,
       created_at, last_used_at
  FROM okg.blobs
 WHERE kind = 'image';

-- Per `redesign-projection-watermarks` (2026-04-25): each
-- `project_batch` writes its observed (source, max_fact_id)
-- pair here inside its own short transaction. The publisher's
-- `dispatch_projection` sweeps these into `source_watermarks`
-- in a single UPDATE under the publisher's advisory lock, then
-- DELETEs the pending rows for this generation. Two batches
-- writing for the same source produce two pending rows; the
-- sweep takes max(new_cursor) per source.
--
-- No row-level UPSERT contention: each batch's INSERT goes to
-- a fresh row keyed by pending_id (BIGSERIAL). Replaces the
-- per-batch ON CONFLICT UPDATE on `source_watermarks` that
-- caused B11 deadlocks at n_workers >= 2.
CREATE TABLE IF NOT EXISTS okg.projection_pending (
  pending_id          BIGSERIAL  PRIMARY KEY,
  generation_id       BIGINT     NOT NULL,
  source              TEXT       NOT NULL,
  new_cursor          BIGINT     NOT NULL,
  batch_index         INTEGER    NOT NULL,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS projection_pending_gen_idx
  ON okg.projection_pending (generation_id);

-- ========== Edge-fact stranding sentinel ==========
--
-- Per `fix-orphan-edge-projection-stranding` (2026-05-19):
-- `project_new_edges` writes one row here for every op='I'
-- edge_fact dropped by the endpoint-visibility filter. The next
-- publish cycle's retry phase re-applies the filter at the new
-- generation and projects rows that now pass, deleting them from
-- this table. Rows that still fail visibility have `last_retry_gen`
-- and `retry_count` bumped so `okg doctor --check
-- edge-fact-strandings` can surface long-stuck strandings as
-- YELLOW (>=2 gens) or RED (>=5 gens).
--
-- `fact_id` is not foreign-keyed against `okg.edge_facts(fact_id)`
-- because that hypertable's PK is composite
-- (fact_id, observed_at); edge_facts is append-only with no row
-- deletion, so a plain BIGINT suffices.

CREATE TABLE IF NOT EXISTS okg.edge_fact_strandings (
  fact_id        BIGINT  PRIMARY KEY,
  first_seen_gen BIGINT  NOT NULL,
  last_retry_gen BIGINT  NOT NULL,
  retry_count    INT     NOT NULL DEFAULT 0,
  reason         TEXT    NOT NULL
                 DEFAULT 'endpoint_invisible_at_projection'
);

CREATE INDEX IF NOT EXISTS edge_fact_strandings_by_first_seen
  ON okg.edge_fact_strandings (first_seen_gen);

-- ---------------------------------------------------------------------------
-- Host-wide connection reservations (unify-connection-budget).
--
-- A process records its DECLARED connection demand here before opening any
-- pool. The per-process admission check can only ask "does my demand fit what
-- is free right now"; two workers booting together each see the other's
-- connections absent and both pass it. A claim recorded before the pools open
-- is what closes that race.
--
-- Rows staler than the caller's horizon are ignored and swept, so a crashed
-- worker does not refuse the restart of that same worker against its own
-- corpse. See src/okg/substrate/db/capacity.py.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS okg.connection_reservations (
    application_name  TEXT        NOT NULL,
    pid               INTEGER     NOT NULL,
    deployment_name        TEXT,
    declared_demand   INTEGER     NOT NULL CHECK (declared_demand >= 0),
    claimed_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    heartbeat_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (application_name, pid)
);

CREATE INDEX IF NOT EXISTS connection_reservations_heartbeat_idx
  ON okg.connection_reservations (heartbeat_at);

-- Issue #2280: the MCP read role has no privilege on this table. It claims,
-- refreshes and releases its own row through the SECURITY DEFINER functions
-- okg.connection_reservation_* (views.sql), which store the SHA-256 of a
-- per-process random holder token here and act only on the row whose digest
-- matches the token the caller presents. NULL for rows written directly by
-- privileged roles (the runtime worker), which those functions never touch.
ALTER TABLE okg.connection_reservations
  ADD COLUMN IF NOT EXISTS holder_token_sha256 BYTEA;

CREATE INDEX IF NOT EXISTS edge_fact_strandings_by_last_retry
  ON okg.edge_fact_strandings (last_retry_gen);

-- ========== Link cache (skip-if-unchanged) ==========

CREATE TABLE IF NOT EXISTS okg.link_cache (
  node_id             TEXT       PRIMARY KEY,
  text_hash           TEXT       NOT NULL,
  extractor_fingerprint TEXT     NOT NULL DEFAULT 'legacy',
  linked_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE okg.link_cache
  ADD COLUMN IF NOT EXISTS extractor_fingerprint TEXT NOT NULL
  DEFAULT 'legacy';

-- ========== Unresolved-route retry table (#685) ==========
--
-- Per `fix-projection-alias-identity` (ruling 1): the projector stamps
-- okg.link_cache as "done" for every cache-miss node it extracts, BEFORE
-- alias resolution has necessarily seen the node's target entities. When
-- the router's `{resolved}` substitution misses (the target alias has not
-- landed yet), that miss is recorded here in the SAME transaction as the
-- link_cache stamp -- so an alias-miss is a retryable strand, not a
-- silently-cached done-ness. A later cycle in which the alias lands matches
-- the new alias value against `entity_value` and DELETEs the matched
-- nodes' link_cache rows (see okg.substrate.catalog.aliases /
-- projection.projector), so exactly those nodes re-extract. Only resolver
-- misses are recorded; no-route and self-loop drops are not retryable
-- (route-table changes already rotate the extractor_fingerprint).
--
-- This table IS the strand record #685 complains was missing. Doctor can
-- report its depth (retry residue = mentions still unresolvable). Rows are
-- scoped by deployment and Chronos branch; only a published checkpoint for
-- that same scope may acknowledge them.
CREATE TABLE IF NOT EXISTS okg.link_retry (
  deployment_name TEXT        NOT NULL,
  branch_name     TEXT        NOT NULL,
  node_id         TEXT        NOT NULL,
  entity_type     TEXT        NOT NULL,
  entity_value    TEXT        NOT NULL,
  recorded_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  ready_at        TIMESTAMPTZ,
  PRIMARY KEY (
    deployment_name, branch_name, node_id, entity_type, entity_value
  )
);
-- Converge databases initialized by an earlier revision of this unmerged PACT.
ALTER TABLE okg.link_retry
  ADD COLUMN IF NOT EXISTS deployment_name TEXT NOT NULL DEFAULT 'default',
  ADD COLUMN IF NOT EXISTS branch_name TEXT NOT NULL DEFAULT 'default',
  ADD COLUMN IF NOT EXISTS ready_at TIMESTAMPTZ;
DO $link_retry_primary_key$
DECLARE
  current_columns TEXT[];
BEGIN
  SELECT array_agg(att.attname ORDER BY keys.ordinality)
    INTO current_columns
    FROM pg_constraint con
    JOIN unnest(con.conkey) WITH ORDINALITY AS keys(attnum, ordinality)
      ON TRUE
    JOIN pg_attribute att
      ON att.attrelid = con.conrelid AND att.attnum = keys.attnum
   WHERE con.conrelid = 'okg.link_retry'::regclass
     AND con.contype = 'p';
  IF current_columns IS DISTINCT FROM ARRAY[
       'deployment_name', 'branch_name', 'node_id', 'entity_type', 'entity_value'
     ]::TEXT[] THEN
    ALTER TABLE okg.link_retry DROP CONSTRAINT IF EXISTS link_retry_pkey;
    ALTER TABLE okg.link_retry ADD PRIMARY KEY
      (deployment_name, branch_name, node_id, entity_type, entity_value);
  END IF;
END
$link_retry_primary_key$;
-- Alias landing and the Publisher's normal no-work probe are both indexed.
CREATE INDEX IF NOT EXISTS link_retry_entity_value_idx
  ON okg.link_retry (entity_value) WHERE ready_at IS NULL;
CREATE INDEX IF NOT EXISTS link_retry_ready_idx
  ON okg.link_retry (deployment_name, branch_name, node_id)
  WHERE ready_at IS NOT NULL;

-- Explicit bounded backfill work for pre-retry historical misses. Unlike
-- link_retry, these rows do not name an entity value; remediation discovers
-- source nodes by stable node_id pages and Publisher acknowledges them only
-- after a published checkpoint.
CREATE TABLE IF NOT EXISTS okg.extractor_retry_queue (
  deployment_name TEXT NOT NULL,
  branch_name     TEXT NOT NULL,
  node_id         TEXT NOT NULL,
  reason          TEXT NOT NULL DEFAULT 'historical_unresolved',
  enqueued_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (deployment_name, branch_name, node_id)
);

CREATE TABLE IF NOT EXISTS okg.projection_remediation_runs (
  run_id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  remediation_kind    TEXT NOT NULL,
  deployment_name     TEXT NOT NULL,
  branch_name         TEXT NOT NULL,
  pinned_checkpoint   TEXT,
  catalog_version_id  BIGINT NOT NULL,
  collision_snapshot  JSONB NOT NULL DEFAULT '[]'::jsonb,
  subtype             TEXT,
  field                TEXT,
  entity_type          TEXT,
  status               TEXT NOT NULL DEFAULT 'running',
  created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  completed_at         TIMESTAMPTZ,
  CONSTRAINT projection_remediation_runs_scope_pin_key UNIQUE
    (remediation_kind, deployment_name, branch_name, subtype, field,
     entity_type, pinned_checkpoint, catalog_version_id)
);
ALTER TABLE okg.projection_remediation_runs
  ADD COLUMN IF NOT EXISTS catalog_version_id BIGINT,
  ADD COLUMN IF NOT EXISTS collision_snapshot JSONB NOT NULL DEFAULT '[]'::jsonb;
UPDATE okg.projection_remediation_runs
   SET catalog_version_id = (
     SELECT max(catalog_version_id) FROM okg.catalog_versions
   )
 WHERE catalog_version_id IS NULL;
ALTER TABLE okg.projection_remediation_runs
  ALTER COLUMN catalog_version_id SET NOT NULL;
DO $projection_remediation_runs_unique$
DECLARE
  constraint_name TEXT;
BEGIN
  FOR constraint_name IN
    SELECT conname FROM pg_constraint
     WHERE conrelid = 'okg.projection_remediation_runs'::regclass
       AND contype = 'u'
  LOOP
    EXECUTE format(
      'ALTER TABLE okg.projection_remediation_runs DROP CONSTRAINT %I',
      constraint_name
    );
  END LOOP;
  ALTER TABLE okg.projection_remediation_runs
    ADD CONSTRAINT projection_remediation_runs_scope_pin_key UNIQUE
      (remediation_kind, deployment_name, branch_name, subtype, field,
       entity_type, pinned_checkpoint, catalog_version_id);
END
$projection_remediation_runs_unique$;

CREATE TABLE IF NOT EXISTS okg.projection_remediation_batches (
  idempotency_key TEXT PRIMARY KEY,
  run_id          UUID NOT NULL
    REFERENCES okg.projection_remediation_runs(run_id),
  input_cursor    TEXT,
  requested_limit INTEGER NOT NULL,
  receipt         JSONB NOT NULL,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ========== Alias-seeding collision records (#686) ==========
--
-- Per `fix-projection-alias-identity` (ruling 2): alias seeding refuses,
-- per value, any alias surface form that maps to more than one distinct
-- canonical node (two 'J. Smith' authors, two same-basename files). Such a
-- value is seeded for NO node (an ambiguous surface form must resolve to
-- nothing) and a typed collision record is persisted here. Seeding/apply
-- output reports it and doctor surfaces it as a standing condition until an
-- operator disambiguates with a scoped alias. Keyed at the catalog-version
-- the collision was detected under (aliases are catalog-version-scoped, not
-- generation-scoped). `canonicals` holds the sorted distinct canonical ids.
CREATE TABLE IF NOT EXISTS okg.alias_collisions (
  catalog_version_id       BIGINT      NOT NULL
    REFERENCES okg.catalog_versions(catalog_version_id),
  subtype                  TEXT        NOT NULL,
  field                    TEXT        NOT NULL,
  alias_value              TEXT        NOT NULL,
  scope                    TEXT,
  canonicals               JSONB       NOT NULL,
  distinct_canonical_count INTEGER     NOT NULL,
  detected_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (catalog_version_id, subtype, field, alias_value)
);

-- ========== Per-enricher progress (incremental dispatch) ==========
--
-- Per `add-substrate-incremental-publish` (2026-04-28): the
-- publisher reads this row before invoking each enricher and
-- advances it after a successful return. `fact_watermark` is
-- the last observed `okg.node_facts.fact_id` (BIGSERIAL);
-- `edge_watermark` is the same on `okg.edge_facts.fact_id`
-- (NOT the hash-derived `edge_id`). `last_status` is
-- `'success'` | `'failed:<exc>'` | `'never'`. A failed run
-- does NOT advance the watermarks; the next publish retries
-- on the same delta.
CREATE TABLE IF NOT EXISTS okg.enricher_progress (
  deployment_name      TEXT       NOT NULL,
  enricher_name   TEXT       NOT NULL,
  fact_watermark  BIGINT     NOT NULL DEFAULT 0,
  edge_watermark  BIGINT     NOT NULL DEFAULT 0,
  generation_id   BIGINT     NOT NULL DEFAULT 0,
  catalog_version_id BIGINT,
  last_run_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_status     TEXT       NOT NULL DEFAULT 'never',
  PRIMARY KEY (deployment_name, enricher_name),
  -- Pin the last_status namespace at the DB so a publisher
  -- typo is rejected at write time rather than silently
  -- absorbed. An operator's `\d okg.enricher_progress`
  -- shows the valid set without reading publisher source.
  -- `withheld:%` (issue #1528) is a run the publisher DECLINED to make: the
  -- enricher's declared input source run is still held off `default`
  -- awaiting an admission decision, so it did not run and its watermark is
  -- held. Deliberately outside `failed:%`,
  -- which `okg doctor --check enricher-liveness` reports as a failing
  -- enricher -- withholding is a correct outcome, not a fault.
  CONSTRAINT enricher_progress_last_status_chk
    CHECK (last_status IN ('never','success')
        OR last_status LIKE 'failed:%'
        OR last_status LIKE 'withheld:%')
);

ALTER TABLE okg.enricher_progress
  ADD COLUMN IF NOT EXISTS catalog_version_id BIGINT;

-- Per `sweep-enricher-liveness` (issue #707): watermarks + last_status
-- alone make a ran-and-emitted-ZERO cycle byte-identical to a productive
-- one, so a broken enricher running at zero LLM calls looked "healthy"
-- for days. `lifetime_edges_emitted` accumulates every cycle's
-- `EnrichResult.n_edges_emitted` (advanced by _advance_enricher_progress);
-- a wired enricher (last_status='success') still sitting at 0 has never
-- produced anything since it was wired. Console diagnostics render that
-- as `wired-but-silent` and `okg doctor --check enricher-liveness` flags
-- it. `dormant_ok` is the explicit declared-liveness escape (design
-- point (i)) for an enricher that is legitimately expected to be quiet;
-- the enricher declares it in code (`dormant_ok` on the enricher class),
-- and every publish advance stamps that declared value onto the row, so
-- the liveness flag is suppressed by declaration, not by silence. (The DB
-- column is publisher-owned: it is overwritten from the enricher's code
-- declaration on every advance, so editing it by hand does not stick.)
-- Both ALTERs are idempotent (schema.sql replay); the
-- DEFAULTs backfill existing rows at migrate time.
ALTER TABLE okg.enricher_progress
  ADD COLUMN IF NOT EXISTS lifetime_edges_emitted BIGINT NOT NULL DEFAULT 0;
ALTER TABLE okg.enricher_progress
  ADD COLUMN IF NOT EXISTS dormant_ok BOOLEAN NOT NULL DEFAULT FALSE;

-- Issue #839: the active unresolved failure streak. Failed/incomplete
-- invocations increment it atomically; a complete success resets it.
ALTER TABLE okg.enricher_progress
  ADD COLUMN IF NOT EXISTS consecutive_failure_count BIGINT NOT NULL DEFAULT 0;
-- Existing active failures predate the counter, so the exact streak length is
-- unknowable. Preserve the sound lower bound instead of presenting a failure
-- as a zero-length streak immediately after migration.
UPDATE okg.enricher_progress
   SET consecutive_failure_count = 1
 WHERE last_status LIKE 'failed:%'
   AND consecutive_failure_count = 0;

-- Guarded for the upgrade path (schema-identity-convergence, migration
-- 0026): schema.sql runs before the ordered migrations, and a
-- CREATE INDEX IF NOT EXISTS resolves its column list BEFORE checking the
-- index name, so naming the renamed column unguarded would abort the
-- migrate on a database 0026 has not reached yet. On upgrade the guard
-- declines and 0026's RENAME COLUMN carries the existing index over.
DO $guard_enricher_progress_deployment_last_run_idx$
BEGIN
  IF EXISTS (
    SELECT 1
      FROM pg_attribute
     WHERE attrelid = 'okg.enricher_progress'::regclass
       AND attname = 'deployment_name'
       AND attnum > 0
       AND NOT attisdropped
  ) THEN
    EXECUTE $idx$
      CREATE INDEX IF NOT EXISTS enricher_progress_deployment_last_run_idx
        ON okg.enricher_progress (deployment_name, last_run_at DESC)
    $idx$;
  END IF;
END
$guard_enricher_progress_deployment_last_run_idx$;

-- ========== Targeted per-input enricher backfill progress ==========
--
-- Issue 214: targeted backfill uses a ledger separate from the normal
-- publisher watermark. Successful chunks mark individual input nodes complete
-- for one deployment/branch/enricher/profile/transform identity; the normal
-- okg.enricher_progress row advances only after the selected enricher's
-- missing-output worklist drains.
CREATE TABLE IF NOT EXISTS okg.enricher_backfill_progress (
  deployment_name          TEXT        NOT NULL,
  branch_name         TEXT        NOT NULL DEFAULT 'default',
  enricher_name       TEXT        NOT NULL,
  input_node_id       TEXT        NOT NULL,
  catalog_version_id  BIGINT      NOT NULL DEFAULT 0,
  profile_fingerprint TEXT        NOT NULL,
  transform_checksum  TEXT        NOT NULL,
  status              TEXT        NOT NULL DEFAULT 'complete',
  attempts            INT         NOT NULL DEFAULT 1,
  emitted_edge_count  BIGINT      NOT NULL DEFAULT 0,
  last_run_id         UUID,
  metadata            JSONB       NOT NULL DEFAULT '{}'::jsonb,
  completed_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (
    deployment_name, branch_name, enricher_name, input_node_id,
    catalog_version_id, profile_fingerprint, transform_checksum
  ),
  CONSTRAINT enricher_backfill_progress_status_chk
    CHECK (status = 'complete'),
  CONSTRAINT enricher_backfill_progress_attempts_chk
    CHECK (attempts > 0),
  CONSTRAINT enricher_backfill_progress_edge_count_chk
    CHECK (emitted_edge_count >= 0)
);

-- Guarded for the upgrade path (schema-identity-convergence, migration
-- 0026): schema.sql runs before the ordered migrations, and a
-- CREATE INDEX IF NOT EXISTS resolves its column list BEFORE checking the
-- index name, so naming the renamed column unguarded would abort the
-- migrate on a database 0026 has not reached yet. On upgrade the guard
-- declines and 0026's RENAME COLUMN carries the existing index over.
DO $guard_enricher_backfill_progress_lookup_idx$
BEGIN
  IF EXISTS (
    SELECT 1
      FROM pg_attribute
     WHERE attrelid = 'okg.enricher_backfill_progress'::regclass
       AND attname = 'deployment_name'
       AND attnum > 0
       AND NOT attisdropped
  ) THEN
    EXECUTE $idx$
      CREATE INDEX IF NOT EXISTS enricher_backfill_progress_lookup_idx
        ON okg.enricher_backfill_progress
        (deployment_name, branch_name, enricher_name, status, updated_at DESC)
    $idx$;
  END IF;
END
$guard_enricher_backfill_progress_lookup_idx$;

-- ========== Post-publish view-edge projection status ==========
--
-- Per `surface-post-publish-projection-failures` (2026-05): the
-- publisher's `_run_post_publish_views` runs native view edge
-- projection AFTER a generation is marked `published`. The
-- projection is intentionally non-blocking (the publish is already
-- final; the view edges are compatibility derivatives). Prior to
-- this table the failure path was `except Exception: log; pass` —
-- a permanent projection bug left compatibility edges silently
-- inconsistent with no machine-readable failure surface.
--
-- Every invocation of `_run_post_publish_views` upserts one row
-- here. `okg doctor` reads this table and reports any row with
-- `status='failed'` as a finding. The shape mirrors
-- `okg.enricher_progress`'s last_status pattern.
CREATE TABLE IF NOT EXISTS okg.post_publish_projection_status (
  deployment_name TEXT       NOT NULL,
  generation_id   BIGINT     NOT NULL,
  last_run_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  status          TEXT       NOT NULL,
  error           TEXT,
  PRIMARY KEY (deployment_name, generation_id),
  CONSTRAINT post_publish_projection_status_status_chk
    CHECK (status IN ('success', 'failed')),
  CONSTRAINT post_publish_projection_status_error_len_chk
    CHECK (error IS NULL OR length(error) <= 4096)
);

CREATE INDEX IF NOT EXISTS post_publish_projection_status_failed_idx
  ON okg.post_publish_projection_status (deployment_name, last_run_at DESC)
  WHERE status = 'failed';

-- ========== Per-source progress (skip-if-unchanged) ==========
--
-- Per `add-substrate-incremental-publish` (2026-04-28): a
-- key/value store distinct from `okg.source_watermarks.last_
-- applied_rev`. Watermarks are one JSON blob per source
-- ("where did I leave off in the stream"); progress is keyed
-- `(deployment, source, key)` so a source like `pdf_text` can
-- store 5,318 per-pub fingerprint rows rather than encode
-- them as one giant JSONB blob. Each source decides what
-- "since" means (file mtime, URL cursor, Kafka offset, SQL
-- row id, …); the publisher passes the dict as
-- `since_progress=` and upserts `progress_after_run()`'s
-- result.
-- NOTE (schema-identity-convergence): this table's bare `deployment` column is
-- the ONE member of the §4 rename batch that does NOT converge here. It is the
-- only renamed relation that views.sql reads (okg.v_source_run_diagnostics and
-- okg.v_source_frontier_state, ~20 references), and views.sql is owned by the
-- concurrent canonical-read-path lane. The column is handed off there rather
-- than renamed across a file two lanes are editing; its registry row in
-- identity_divergences.yaml records the handoff.
CREATE TABLE IF NOT EXISTS okg.source_progress (
  deployment    TEXT       NOT NULL,
  source_name   TEXT       NOT NULL,
  key           TEXT       NOT NULL,
  value         TEXT       NOT NULL,
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (deployment, source_name, key)
);

CREATE INDEX IF NOT EXISTS source_progress_deployment_updated_idx
  ON okg.source_progress (deployment, updated_at DESC);

-- Source rollback snapshots only need adapter-owned progress rows.
-- Diagnostic rows under the reserved sync.* namespace can grow with
-- run history and must not make rollback-critical snapshots scan the
-- telemetry slice for a source.
CREATE INDEX IF NOT EXISTS source_progress_adapter_state_idx
  ON okg.source_progress (deployment, source_name, key)
  WHERE key NOT LIKE 'sync.%';

-- ========== MCP tool-call tracing ==========
--
-- Every MCP tool invocation appends one row. Hypertable so
-- retention + compression policies keep the footprint bounded.
-- Substrate-native replacement for the legacy JSONL trace file.

CREATE TABLE IF NOT EXISTS okg.mcp_trace_events (
  event_id         BIGSERIAL,
  session_id       TEXT        NOT NULL,
  deployment_name  TEXT        NOT NULL,
  generation_id    TEXT,
  tool_name        TEXT        NOT NULL,
  question_label   TEXT,
  request_json     JSONB,
  response_json    JSONB,
  error_text       TEXT,
  duration_ms      INTEGER,
  event_time       TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (event_time, event_id)
);

-- generation_id became TEXT with the Chronos cutover (a published generation
-- is now a Chronos checkpoint with a non-numeric string id). Fresh DBs already
-- CREATE it as TEXT (above), so this ALTER only exists to convert ancient
-- pre-Chronos DBs where it was bigint. Guard it: ALTER COLUMN ... TYPE forces a
-- table rewrite, which TimescaleDB rejects on a hypertable that has compressed
-- chunks ("operation not supported on hypertables with compressed chunks").
-- Because schema.sql is re-executed wholesale on every migrate (the idempotency
-- contract), an unconditional ALTER breaks `okg migrate` on any aged deployment
-- once the 7-day compression policy has compressed an mcp_trace_events chunk.
-- Skipping when the column is already TEXT issues zero rewrite DDL on every
-- current-schema DB. (The node_facts/edge_facts sites below are already
-- compression-safe; this was the one ALTER COLUMN TYPE that wasn't.)
DO $$
BEGIN
  IF EXISTS (
    SELECT 1
      FROM information_schema.columns
     WHERE table_schema = 'okg'
       AND table_name   = 'mcp_trace_events'
       AND column_name  = 'generation_id'
       AND data_type   <> 'text'
  ) THEN
    ALTER TABLE okg.mcp_trace_events
      ALTER COLUMN generation_id TYPE TEXT USING generation_id::text;
  END IF;
END $$;

SELECT create_hypertable(
  'okg.mcp_trace_events', 'event_time',
  chunk_time_interval => INTERVAL '1 day',
  if_not_exists => TRUE,
  migrate_data => TRUE
);

CREATE INDEX IF NOT EXISTS mcp_trace_session_time_idx
  ON okg.mcp_trace_events (session_id, event_time);
CREATE INDEX IF NOT EXISTS mcp_trace_tool_time_idx
  ON okg.mcp_trace_events (tool_name, event_time);
CREATE INDEX IF NOT EXISTS mcp_trace_deployment_time_idx
  ON okg.mcp_trace_events (deployment_name, event_time);

-- AKMON Phase A.3 cut 2c: branch_name on every MCP trace event so
-- operator dashboards can split by branch (which sessions read
-- from default vs experiment vs quarantine). Nullable + default
-- 'default' so existing rows + traces from legacy callers remain
-- meaningful.
ALTER TABLE okg.mcp_trace_events
  ADD COLUMN IF NOT EXISTS branch_name TEXT DEFAULT 'default';

CREATE INDEX IF NOT EXISTS mcp_trace_branch_time_idx
  ON okg.mcp_trace_events (branch_name, event_time);

-- AKMON Phase 6.2 (add-agent-okg-route-recommendations): route_id
-- attributes a tool call back to the route recommendation that led
-- to it, and route_manifest_checksum pins the manifest version so
-- post-hoc analysis can tell when a route's definition changed.
-- Both nullable so legacy + non-route-following calls remain valid.
ALTER TABLE okg.mcp_trace_events
  ADD COLUMN IF NOT EXISTS route_id TEXT;
ALTER TABLE okg.mcp_trace_events
  ADD COLUMN IF NOT EXISTS route_manifest_checksum TEXT;

CREATE INDEX IF NOT EXISTS mcp_trace_route_time_idx
  ON okg.mcp_trace_events (route_id, event_time)
  WHERE route_id IS NOT NULL;

ALTER TABLE okg.mcp_trace_events SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'deployment_name',
  timescaledb.compress_orderby   = 'event_time DESC'
);

SELECT add_compression_policy(
  'okg.mcp_trace_events', INTERVAL '7 days', if_not_exists => TRUE
);
SELECT add_retention_policy(
  'okg.mcp_trace_events', INTERVAL '30 days', if_not_exists => TRUE
);

CREATE OR REPLACE VIEW okg.trace_sessions AS
SELECT
  session_id,
  deployment_name,
  min(event_time)          AS first_call_at,
  max(event_time)          AS last_call_at,
  count(*)                 AS tool_call_count,
  count(DISTINCT tool_name) AS unique_tool_count,
  count(*) FILTER (WHERE error_text IS NOT NULL) AS error_count,
  sum(duration_ms)         AS total_duration_ms
FROM okg.mcp_trace_events
GROUP BY session_id, deployment_name;

CREATE OR REPLACE VIEW okg.trace_sessions_labeled AS
SELECT
  session_id,
  deployment_name,
  question_label,
  min(event_time)          AS first_call_at,
  max(event_time)          AS last_call_at,
  count(*)                 AS tool_call_count,
  count(DISTINCT tool_name) AS unique_tool_count,
  count(*) FILTER (WHERE error_text IS NOT NULL) AS error_count,
  sum(duration_ms)         AS total_duration_ms
FROM okg.mcp_trace_events
GROUP BY session_id, deployment_name, question_label;

-- Consecutive repeated node references within a short window —
-- a navigability / scent-loss signal.
CREATE OR REPLACE VIEW okg.trace_backtracks AS
WITH ordered AS (
  SELECT
    session_id,
    tool_name,
    request_json->>'node_id' AS node_id,
    event_time,
    lag(request_json->>'node_id') OVER w AS prev_node_id,
    lag(event_time)              OVER w AS prev_event_time
  FROM okg.mcp_trace_events
  WHERE request_json ? 'node_id'
  WINDOW w AS (PARTITION BY session_id ORDER BY event_time)
)
SELECT
  session_id,
  count(*) AS backtrack_count
FROM ordered
WHERE node_id = prev_node_id
  AND event_time - prev_event_time < INTERVAL '60 seconds'
GROUP BY session_id;

-- ========== Unified AKMON telemetry ==========
--
-- OpenTelemetry is the instrumentation API; these OKG-owned tables
-- are the durable local store. Raw spans/events/metrics stay here,
-- while graph sources project only curated summaries.

CREATE TABLE IF NOT EXISTS okg.telemetry_traces (
  trace_id         TEXT        PRIMARY KEY,
  deployment_name  TEXT        NOT NULL,
  branch_name      TEXT        NOT NULL DEFAULT 'default',
  root_name        TEXT        NOT NULL,
  root_kind        TEXT        NOT NULL,
  status_code      TEXT        NOT NULL DEFAULT 'unset',
  status_message   TEXT,
  started_at       TIMESTAMPTZ NOT NULL,
  ended_at         TIMESTAMPTZ,
  duration_ms      INTEGER,
  generation_id    TEXT,
  correlation_id   TEXT,
  source_name      TEXT,
  workflow_id      TEXT,
  session_id       TEXT,
  attributes       JSONB       NOT NULL DEFAULT '{}'::jsonb,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE okg.telemetry_traces
  DROP CONSTRAINT IF EXISTS telemetry_traces_pkey;
ALTER TABLE okg.telemetry_traces
  ADD CONSTRAINT telemetry_traces_pkey
  PRIMARY KEY (deployment_name, trace_id);

CREATE INDEX IF NOT EXISTS telemetry_traces_deployment_started_idx
  ON okg.telemetry_traces (deployment_name, started_at DESC);
CREATE INDEX IF NOT EXISTS telemetry_traces_generation_idx
  ON okg.telemetry_traces (generation_id)
  WHERE generation_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS okg.telemetry_spans (
  event_time          TIMESTAMPTZ NOT NULL DEFAULT now(),
  trace_id            TEXT        NOT NULL,
  span_id             TEXT        NOT NULL,
  parent_span_id      TEXT,
  span_name           TEXT        NOT NULL,
  span_kind           TEXT        NOT NULL DEFAULT 'internal',
  status_code         TEXT        NOT NULL DEFAULT 'unset',
  status_message      TEXT,
  started_at          TIMESTAMPTZ NOT NULL,
  ended_at            TIMESTAMPTZ,
  duration_ms         INTEGER,
  deployment_name     TEXT        NOT NULL,
  branch_name         TEXT        NOT NULL DEFAULT 'default',
  generation_id       TEXT,
  correlation_id      TEXT,
  source_name         TEXT,
  workflow_id         TEXT,
  run_id              TEXT,
  tool_name           TEXT,
  route_id            TEXT,
  session_id          TEXT,
  attributes          JSONB       NOT NULL DEFAULT '{}'::jsonb,
  payload_summary     JSONB       NOT NULL DEFAULT '{}'::jsonb,
  unavailable_reason  TEXT,
  PRIMARY KEY (event_time, trace_id, span_id)
);

SELECT create_hypertable(
  'okg.telemetry_spans', 'event_time',
  chunk_time_interval => INTERVAL '1 day',
  if_not_exists => TRUE,
  migrate_data => TRUE
);

CREATE INDEX IF NOT EXISTS telemetry_spans_trace_time_idx
  ON okg.telemetry_spans (trace_id, event_time);
CREATE INDEX IF NOT EXISTS telemetry_spans_deployment_time_idx
  ON okg.telemetry_spans (deployment_name, event_time DESC);
CREATE INDEX IF NOT EXISTS telemetry_spans_tool_time_idx
  ON okg.telemetry_spans (tool_name, event_time DESC)
  WHERE tool_name IS NOT NULL;
CREATE INDEX IF NOT EXISTS telemetry_spans_generation_idx
  ON okg.telemetry_spans (generation_id)
  WHERE generation_id IS NOT NULL;

ALTER TABLE okg.telemetry_spans SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'deployment_name',
  timescaledb.compress_orderby   = 'event_time DESC'
);

SELECT add_compression_policy(
  'okg.telemetry_spans', INTERVAL '7 days', if_not_exists => TRUE
);
SELECT add_retention_policy(
  'okg.telemetry_spans', INTERVAL '30 days', if_not_exists => TRUE
);

CREATE TABLE IF NOT EXISTS okg.telemetry_events (
  event_id            BIGSERIAL,
  event_time          TIMESTAMPTZ NOT NULL DEFAULT now(),
  trace_id            TEXT        NOT NULL,
  span_id             TEXT,
  event_name          TEXT        NOT NULL,
  severity            TEXT,
  deployment_name     TEXT,
  attributes          JSONB       NOT NULL DEFAULT '{}'::jsonb,
  unavailable_reason  TEXT,
  PRIMARY KEY (event_time, event_id)
);

SELECT create_hypertable(
  'okg.telemetry_events', 'event_time',
  chunk_time_interval => INTERVAL '1 day',
  if_not_exists => TRUE,
  migrate_data => TRUE
);

CREATE INDEX IF NOT EXISTS telemetry_events_trace_time_idx
  ON okg.telemetry_events (trace_id, event_time);
CREATE INDEX IF NOT EXISTS telemetry_events_name_time_idx
  ON okg.telemetry_events (event_name, event_time DESC);

-- Bounded, identity-bearing graph changes captured at the publisher boundary.
-- The manifest carries coverage/count semantics; rows are independently paged
-- so large publishes never overflow telemetry_spans.payload_summary.
CREATE TABLE IF NOT EXISTS okg.telemetry_graph_effect_manifests (
  deployment_name      TEXT        NOT NULL,
  trace_id             TEXT        NOT NULL,
  span_id              TEXT        NOT NULL,
  branch_name          TEXT        NOT NULL DEFAULT 'default',
  producer_key         TEXT,
  input_generation_id  TEXT,
  output_generation_id TEXT,
  coverage              TEXT        NOT NULL
    CHECK (coverage IN ('exact', 'sampled', 'aggregate_only', 'missing')),
  unavailable_reason   TEXT,
  total_count          BIGINT      NOT NULL DEFAULT 0,
  retained_count       INTEGER     NOT NULL DEFAULT 0,
  retention_limit      INTEGER     NOT NULL DEFAULT 0,
  counts               JSONB       NOT NULL DEFAULT '{}'::jsonb,
  recorded_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (deployment_name, trace_id, span_id)
);

CREATE TABLE IF NOT EXISTS okg.telemetry_graph_effects (
  effect_id        BIGSERIAL PRIMARY KEY,
  deployment_name TEXT        NOT NULL,
  trace_id         TEXT        NOT NULL,
  span_id          TEXT        NOT NULL,
  sequence         INTEGER     NOT NULL,
  object_kind      TEXT        NOT NULL CHECK (object_kind IN ('node', 'edge')),
  object_id        TEXT        NOT NULL,
  change_kind      TEXT        NOT NULL CHECK (change_kind IN ('added', 'updated', 'retracted')),
  object_type      TEXT,
  source_id        TEXT,
  target_id        TEXT,
  recorded_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (deployment_name, trace_id, span_id, sequence)
);

CREATE INDEX IF NOT EXISTS telemetry_graph_effects_trace_sequence_idx
  ON okg.telemetry_graph_effects (deployment_name, trace_id, span_id, sequence);

ALTER TABLE okg.telemetry_events SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'event_name',
  timescaledb.compress_orderby   = 'event_time DESC'
);

SELECT add_compression_policy(
  'okg.telemetry_events', INTERVAL '7 days', if_not_exists => TRUE
);
SELECT add_retention_policy(
  'okg.telemetry_events', INTERVAL '30 days', if_not_exists => TRUE
);

CREATE TABLE IF NOT EXISTS okg.telemetry_metrics (
  metric_id       BIGSERIAL,
  metric_time     TIMESTAMPTZ NOT NULL DEFAULT now(),
  metric_name     TEXT        NOT NULL,
  metric_kind     TEXT        NOT NULL,
  value_double    DOUBLE PRECISION,
  value_int       BIGINT,
  unit            TEXT,
  deployment_name TEXT        NOT NULL,
  branch_name     TEXT        NOT NULL DEFAULT 'default',
  trace_id        TEXT,
  span_id         TEXT,
  labels          JSONB       NOT NULL DEFAULT '{}'::jsonb,
  PRIMARY KEY (metric_time, metric_id)
);

SELECT create_hypertable(
  'okg.telemetry_metrics', 'metric_time',
  chunk_time_interval => INTERVAL '1 day',
  if_not_exists => TRUE,
  migrate_data => TRUE
);

CREATE INDEX IF NOT EXISTS telemetry_metrics_name_time_idx
  ON okg.telemetry_metrics (metric_name, metric_time DESC);
CREATE INDEX IF NOT EXISTS telemetry_metrics_deployment_time_idx
  ON okg.telemetry_metrics (deployment_name, metric_time DESC);
CREATE INDEX IF NOT EXISTS telemetry_metrics_trace_idx
  ON okg.telemetry_metrics (trace_id)
  WHERE trace_id IS NOT NULL;

ALTER TABLE okg.telemetry_metrics SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'metric_name',
  timescaledb.compress_orderby   = 'metric_time DESC'
);

SELECT add_compression_policy(
  'okg.telemetry_metrics', INTERVAL '7 days', if_not_exists => TRUE
);
SELECT add_retention_policy(
  'okg.telemetry_metrics', INTERVAL '30 days', if_not_exists => TRUE
);

CREATE TABLE IF NOT EXISTS okg.telemetry_artifacts (
  artifact_id         TEXT        PRIMARY KEY,
  deployment_name     TEXT        NOT NULL,
  trace_id            TEXT        NOT NULL,
  span_id             TEXT,
  artifact_kind       TEXT        NOT NULL,
  path                TEXT        NOT NULL,
  format              TEXT        NOT NULL,
  collector           TEXT,
  collector_status    TEXT        NOT NULL,
  unavailable_reason  TEXT,
  summary             JSONB       NOT NULL DEFAULT '{}'::jsonb,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE okg.telemetry_artifacts
  ADD COLUMN IF NOT EXISTS deployment_name TEXT;
UPDATE okg.telemetry_artifacts a
   SET deployment_name = t.deployment_name
  FROM okg.telemetry_traces t
 WHERE a.deployment_name IS NULL
   AND a.trace_id = t.trace_id;
ALTER TABLE okg.telemetry_artifacts
  ALTER COLUMN deployment_name SET NOT NULL;

CREATE INDEX IF NOT EXISTS telemetry_artifacts_trace_idx
  ON okg.telemetry_artifacts (deployment_name, trace_id, created_at DESC);

CREATE OR REPLACE VIEW okg.v_telemetry_trace_summary AS
SELECT
  t.trace_id,
  t.deployment_name,
  t.branch_name,
  t.root_name,
  t.root_kind,
  t.status_code,
  t.started_at,
  t.ended_at,
  t.duration_ms,
  t.generation_id,
  t.correlation_id,
  t.source_name,
  t.workflow_id,
  t.session_id,
  count(DISTINCT s.span_id) AS span_count,
  max(s.duration_ms) AS slowest_span_ms,
  (array_agg(s.span_name ORDER BY s.duration_ms DESC NULLS LAST))[1]
    AS slowest_span_name,
  count(DISTINCT a.artifact_id) AS artifact_count
FROM okg.telemetry_traces t
LEFT JOIN okg.telemetry_spans s
  ON s.deployment_name = t.deployment_name
 AND s.trace_id = t.trace_id
LEFT JOIN okg.telemetry_artifacts a
  ON a.deployment_name = t.deployment_name
 AND a.trace_id = t.trace_id
GROUP BY
  t.trace_id, t.deployment_name, t.branch_name, t.root_name, t.root_kind,
  t.status_code, t.started_at, t.ended_at, t.duration_ms, t.generation_id,
  t.correlation_id, t.source_name, t.workflow_id, t.session_id;

CREATE OR REPLACE VIEW okg.v_telemetry_span_tree AS
SELECT
  trace_id,
  span_id,
  parent_span_id,
  span_name,
  span_kind,
  status_code,
  deployment_name,
  branch_name,
  started_at,
  ended_at,
  duration_ms,
  generation_id,
  source_name,
  workflow_id,
  run_id,
  tool_name,
  route_id,
  session_id,
  attributes,
  payload_summary,
  unavailable_reason
FROM okg.telemetry_spans;

-- ========== Grants ==========
--
-- `schema-grant-coherence`: the grant surface for the two externally-exposed
-- roles (`app_ro`, `okg_mcp`) is DATA, not a naming convention and not a
-- statement order. `okg.grant_registry` is that data; `views.sql` seeds the
-- substrate rows, `okg.apply_grant_registry()` (defined in views.sql) is the
-- only thing that issues the GRANTs, and both `views.sql` and
-- `runtime/sql/role_split.sql` call it. Two scripts converging on ONE registry
-- is what makes them commute: whichever ran last, the effective privilege set
-- is the registry.
--
-- Column semantics:
--   relation       schema-qualified relation name, or a schema-qualified LIKE
--                  pattern when `is_pattern` (Chronos creates its physical
--                  `public._chronos*` tables lazily at publish time, so they
--                  cannot be enumerated by name at install).
--   object_kind    'TABLE' (tables, views, partitioned tables) or 'SEQUENCE'.
--   role_name      grantee, or NULL for a classification-only row.
--   privileges     the privilege list, or NULL for a classification-only row.
--   classification 'contract' — granted, and part of the published read
--                  surface; 'internal' — NEVER granted, asserted absent from
--                  every governed role's privilege set by the drift check.
--   source         'substrate' (owned by views.sql, replaced wholesale on
--                  every install) or anything else (deployment extension
--                  point; never touched by the substrate seed).
--
-- The drift check `okg.grant_registry_drift()` is the loud half: a governed
-- role holding a privilege with no registry row, an `okg.v_*` view with no
-- registry row, or an `internal` relation that became readable, are each a
-- named finding rather than a silent widening.
CREATE TABLE IF NOT EXISTS okg.grant_registry (
  entry_ref       TEXT PRIMARY KEY,
  relation        TEXT    NOT NULL,
  object_kind     TEXT    NOT NULL DEFAULT 'TABLE',
  role_name       TEXT,
  privileges      TEXT,
  classification  TEXT    NOT NULL DEFAULT 'contract',
  is_pattern      BOOLEAN NOT NULL DEFAULT FALSE,
  source          TEXT    NOT NULL DEFAULT 'substrate',
  note            TEXT,
  CONSTRAINT grant_registry_object_kind_known
    CHECK (object_kind IN ('TABLE', 'SEQUENCE')),
  CONSTRAINT grant_registry_classification_known
    CHECK (classification IN ('contract', 'internal')),
  -- A contract row grants something to someone; an internal row grants
  -- nothing to no one. Anything in between is an authoring mistake, refused
  -- at insert rather than discovered as a missing or surprise privilege.
  CONSTRAINT grant_registry_classification_shape
    CHECK (
      (classification = 'contract'
        AND role_name IS NOT NULL AND privileges IS NOT NULL)
      OR
      (classification = 'internal'
        AND role_name IS NULL AND privileges IS NULL)
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS grant_registry_grant_key
  ON okg.grant_registry (relation, object_kind, role_name)
  WHERE role_name IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS grant_registry_internal_key
  ON okg.grant_registry (relation, object_kind)
  WHERE role_name IS NULL;

-- `schema-grant-coherence` req.scoped-catchup: a grant the runtime could not
-- apply is DURABLE, not a server-log warning. The Chronos epoch hook
-- (graph_store_internal/schema.py) enqueues here when its bounded retry loop
-- exhausts on `tuple concurrently updated`, drains the queue on its next run,
-- and `okg doctor --check pending-grants` reports whatever is still queued.
-- Warn-and-forget is how a dropped privilege resurfaces later as an unrelated
-- permission-denied in a different subsystem.
CREATE TABLE IF NOT EXISTS okg.pending_grants (
  statement       TEXT PRIMARY KEY,
  first_failed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  attempts        INTEGER     NOT NULL DEFAULT 1,
  last_error      TEXT
);

-- ========== External-backed nodes (issue #83) ==========
--
-- A node may carry an external HANDLE (an in-attrs typed block, see
-- okg.substrate.external_backed). Two stores back it, BOTH outside the
-- Chronos versioned graph and branch-agnostic (precedent: okg.link_cache,
-- which is node_id-keyed and joined to the versioned surface):
--
--  * okg.node_series      — raw high-frequency samples (TimescaleDB hypertable
--                           + retention), keyed by node_id+metric. Filled by a
--                           collector or telemetry; read for `latest_from_series`
--                           hydration and by the rollup source.
--  * okg.external_probe_cache — latest `probe` result per (node_id,
--                           backing_fingerprint, field). The fingerprint (NOT
--                           node_id alone) is in the key so divergent per-branch
--                           handles never read each other's cached probe.
--
-- INVARIANT: neither table is ever part of a published generation. The only
-- path from a sample into a graph fact is the rollup source (admission).

CREATE TABLE IF NOT EXISTS okg.node_series (
  sample_id        BIGSERIAL,
  deployment_name  TEXT             NOT NULL,
  node_id          TEXT             NOT NULL,
  metric           TEXT             NOT NULL,
  observed_at      TIMESTAMPTZ      NOT NULL DEFAULT now(),
  value            DOUBLE PRECISION,
  source           TEXT,
  status           TEXT             NOT NULL DEFAULT 'ok'
    CHECK (status IN ('ok', 'unavailable', 'error')),
  attrs            JSONB            NOT NULL DEFAULT '{}'::jsonb,
  PRIMARY KEY (observed_at, sample_id)
);

SELECT create_hypertable(
  'okg.node_series', 'observed_at',
  chunk_time_interval => INTERVAL '1 day',
  if_not_exists => TRUE,
  migrate_data => TRUE
);

CREATE INDEX IF NOT EXISTS node_series_node_metric_time_idx
  ON okg.node_series (deployment_name, node_id, metric, observed_at DESC);

ALTER TABLE okg.node_series SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'deployment_name',
  timescaledb.compress_orderby   = 'observed_at DESC'
);

SELECT add_compression_policy(
  'okg.node_series', INTERVAL '7 days', if_not_exists => TRUE
);
SELECT add_retention_policy(
  'okg.node_series', INTERVAL '30 days', if_not_exists => TRUE
);

-- Latest `probe` reading per (node_id, backing_fingerprint, field). Mutable
-- UPSERT; NOT a hypertable. The backing_fingerprint in the PK is load-bearing:
-- two branches with divergent handles for the same node must not collide.
CREATE TABLE IF NOT EXISTS okg.external_probe_cache (
  deployment_name     TEXT        NOT NULL,
  node_id             TEXT        NOT NULL,
  backing_fingerprint TEXT        NOT NULL,
  field               TEXT        NOT NULL,
  observed_at         TIMESTAMPTZ NOT NULL,
  expires_at          TIMESTAMPTZ NOT NULL,
  status              TEXT        NOT NULL
    CHECK (status IN ('ok', 'unavailable', 'denied', 'error', 'timeout')),
  payload             JSONB,
  error               JSONB,
  trust_label         TEXT        NOT NULL DEFAULT 'untrusted',
  provenance          JSONB       NOT NULL DEFAULT '{}'::jsonb,
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (deployment_name, node_id, backing_fingerprint, field)
);

CREATE INDEX IF NOT EXISTS external_probe_cache_node_idx
  ON okg.external_probe_cache (deployment_name, node_id);

-- Per-(node_id, metric) earliest-retained watermark (external-backed-nodes
-- rollups, M3). `first_observed_at` is the earliest sample EVER recorded
-- (monotonic-min, maintained by the series writer); it survives retention
-- eviction of node_series chunks. A series read uses it to distinguish
-- `aged_out` (a range that existed but was evicted) from `no_data` (a range
-- that never existed, or a gap within the retained window). Coercing
-- empty-equals-aged_out is forbidden (default-value-as-fallback is a P0).
CREATE TABLE IF NOT EXISTS okg.node_series_watermark (
  deployment_name    TEXT        NOT NULL,
  node_id            TEXT        NOT NULL,
  metric             TEXT        NOT NULL,
  first_observed_at  TIMESTAMPTZ NOT NULL,
  updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (deployment_name, node_id, metric)
);

-- ========== Operator call samples (issue #2011, PACT call-sample-policy) ==========
--
-- Every node-targeted operator call leaves ONE sample, under a per-relation
-- policy in {none, metadata, inline, digest} (see
-- okg.substrate.external_backed.call_sample). Graph reads default to `none`
-- and therefore write nothing; a live relation declares its own policy. A call
-- with no target node (search) is telemetry-only and writes no sample here.
--
-- `none` is never a stored row — it means "do not sample" — so the stored
-- `policy` domain is the three writing policies only.
--
-- Three NAMED retention classes, one hypertable each, with DISTINCT retention
-- and compression policies (decision 4, Sprint 20 charter #2008):
--
--   hot   — high-frequency operational probes;  7 days retained.
--   warm  — routine live-relation reads;       90 days retained.
--   cold  — audit-grade / low-volume calls;   730 days retained.
--
-- A relation declares its class. Like okg.node_series these tables are OUTSIDE
-- the Chronos versioned graph and branch-agnostic.
--
-- INVARIANT: no sample table is ever part of a published generation. The only
-- paths from a sample into a graph fact are the rollup source and an agent
-- record through admission.
CREATE TABLE IF NOT EXISTS okg.call_sample_hot (
  sample_id        BIGSERIAL,
  deployment_name  TEXT        NOT NULL,
  node_id          TEXT        NOT NULL,
  relation         TEXT        NOT NULL,
  observed_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  status           TEXT        NOT NULL
    CHECK (status IN ('ok', 'unavailable', 'denied', 'error', 'timeout')),
  -- Identity. principal / decision / trace_id / session_id are NOT NULL and
  -- non-blank BY DESIGN: a live call that cannot name who asked and what the
  -- gate decided must fail loudly here rather than store a blank that reads
  -- later as "nobody, allowed".
  principal        TEXT        NOT NULL CHECK (btrim(principal) <> ''),
  decision         TEXT        NOT NULL CHECK (decision IN ('allow', 'deny')),
  trace_id         TEXT        NOT NULL CHECK (btrim(trace_id) <> ''),
  session_id       TEXT        NOT NULL CHECK (btrim(session_id) <> ''),
  tool_name        TEXT,
  -- `policy` is what this row actually stored; `declared_policy` is what the
  -- relation asked for. They differ exactly when an over-cap inline payload
  -- degraded to digest, so the degradation is readable and never silent.
  policy           TEXT        NOT NULL
    CHECK (policy IN ('metadata', 'inline', 'digest')),
  declared_policy  TEXT        NOT NULL
    CHECK (declared_policy IN ('metadata', 'inline', 'digest')),
  payload          JSONB,
  payload_sha256   TEXT,
  payload_bytes    INTEGER CHECK (payload_bytes IS NULL OR payload_bytes >= 0),
  summary          TEXT,
  -- Where a digested payload can be fetched again: {query, source}. `query`
  -- carries the call's argument SHAPE, and an argument's VALUE only when the
  -- relation named that key in its refetch_query_keys allowlist (see
  -- okg.substrate.external_backed.call_sample.refetch_pointer_for_call). A
  -- caller's arguments are NOT copied in wholesale: a cold-class row lives for
  -- 730 days, and a credential or a person's name passed as a tool argument
  -- would live here for all of it.
  refetch          JSONB,
  attrs            JSONB       NOT NULL DEFAULT '{}'::jsonb,
  PRIMARY KEY (observed_at, sample_id),
  -- inline never exceeds the 16 KiB cap; over-cap payloads degrade to digest
  -- in the writer.
  CONSTRAINT call_sample_hot_inline_cap CHECK (
    policy <> 'inline' OR payload_bytes IS NULL OR payload_bytes <= 16384
  ),
  -- a digest row stores no payload ...
  CONSTRAINT call_sample_hot_digest_no_payload CHECK (
    policy <> 'digest' OR payload IS NULL
  ),
  -- ... and whenever it records that a payload existed, it must carry the
  -- sha256 and a re-fetch pointer holding query and source.
  CONSTRAINT call_sample_hot_digest_pointer CHECK (
    policy <> 'digest'
    OR payload_bytes IS NULL
    OR (payload_sha256 IS NOT NULL AND refetch ? 'query' AND refetch ? 'source')
  ),
  -- metadata keeps no payload bytes at all.
  CONSTRAINT call_sample_hot_metadata_bare CHECK (
    policy <> 'metadata' OR (payload IS NULL AND payload_sha256 IS NULL)
  )
);

CREATE INDEX IF NOT EXISTS call_sample_hot_node_relation_time_idx
  ON okg.call_sample_hot (deployment_name, node_id, relation, observed_at DESC);
CREATE INDEX IF NOT EXISTS call_sample_hot_session_trace_idx
  ON okg.call_sample_hot (session_id, trace_id);

CREATE TABLE IF NOT EXISTS okg.call_sample_warm (
  sample_id        BIGSERIAL,
  deployment_name  TEXT        NOT NULL,
  node_id          TEXT        NOT NULL,
  relation         TEXT        NOT NULL,
  observed_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  status           TEXT        NOT NULL
    CHECK (status IN ('ok', 'unavailable', 'denied', 'error', 'timeout')),
  -- Identity. principal / decision / trace_id / session_id are NOT NULL and
  -- non-blank BY DESIGN: a live call that cannot name who asked and what the
  -- gate decided must fail loudly here rather than store a blank that reads
  -- later as "nobody, allowed".
  principal        TEXT        NOT NULL CHECK (btrim(principal) <> ''),
  decision         TEXT        NOT NULL CHECK (decision IN ('allow', 'deny')),
  trace_id         TEXT        NOT NULL CHECK (btrim(trace_id) <> ''),
  session_id       TEXT        NOT NULL CHECK (btrim(session_id) <> ''),
  tool_name        TEXT,
  -- `policy` is what this row actually stored; `declared_policy` is what the
  -- relation asked for. They differ exactly when an over-cap inline payload
  -- degraded to digest, so the degradation is readable and never silent.
  policy           TEXT        NOT NULL
    CHECK (policy IN ('metadata', 'inline', 'digest')),
  declared_policy  TEXT        NOT NULL
    CHECK (declared_policy IN ('metadata', 'inline', 'digest')),
  payload          JSONB,
  payload_sha256   TEXT,
  payload_bytes    INTEGER CHECK (payload_bytes IS NULL OR payload_bytes >= 0),
  summary          TEXT,
  -- Where a digested payload can be fetched again: {query, source}. `query`
  -- carries the call's argument SHAPE, and an argument's VALUE only when the
  -- relation named that key in its refetch_query_keys allowlist (see
  -- okg.substrate.external_backed.call_sample.refetch_pointer_for_call). A
  -- caller's arguments are NOT copied in wholesale: a cold-class row lives for
  -- 730 days, and a credential or a person's name passed as a tool argument
  -- would live here for all of it.
  refetch          JSONB,
  attrs            JSONB       NOT NULL DEFAULT '{}'::jsonb,
  PRIMARY KEY (observed_at, sample_id),
  -- inline never exceeds the 16 KiB cap; over-cap payloads degrade to digest
  -- in the writer.
  CONSTRAINT call_sample_warm_inline_cap CHECK (
    policy <> 'inline' OR payload_bytes IS NULL OR payload_bytes <= 16384
  ),
  -- a digest row stores no payload ...
  CONSTRAINT call_sample_warm_digest_no_payload CHECK (
    policy <> 'digest' OR payload IS NULL
  ),
  -- ... and whenever it records that a payload existed, it must carry the
  -- sha256 and a re-fetch pointer holding query and source.
  CONSTRAINT call_sample_warm_digest_pointer CHECK (
    policy <> 'digest'
    OR payload_bytes IS NULL
    OR (payload_sha256 IS NOT NULL AND refetch ? 'query' AND refetch ? 'source')
  ),
  -- metadata keeps no payload bytes at all.
  CONSTRAINT call_sample_warm_metadata_bare CHECK (
    policy <> 'metadata' OR (payload IS NULL AND payload_sha256 IS NULL)
  )
);

CREATE INDEX IF NOT EXISTS call_sample_warm_node_relation_time_idx
  ON okg.call_sample_warm (deployment_name, node_id, relation, observed_at DESC);
CREATE INDEX IF NOT EXISTS call_sample_warm_session_trace_idx
  ON okg.call_sample_warm (session_id, trace_id);

CREATE TABLE IF NOT EXISTS okg.call_sample_cold (
  sample_id        BIGSERIAL,
  deployment_name  TEXT        NOT NULL,
  node_id          TEXT        NOT NULL,
  relation         TEXT        NOT NULL,
  observed_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  status           TEXT        NOT NULL
    CHECK (status IN ('ok', 'unavailable', 'denied', 'error', 'timeout')),
  -- Identity. principal / decision / trace_id / session_id are NOT NULL and
  -- non-blank BY DESIGN: a live call that cannot name who asked and what the
  -- gate decided must fail loudly here rather than store a blank that reads
  -- later as "nobody, allowed".
  principal        TEXT        NOT NULL CHECK (btrim(principal) <> ''),
  decision         TEXT        NOT NULL CHECK (decision IN ('allow', 'deny')),
  trace_id         TEXT        NOT NULL CHECK (btrim(trace_id) <> ''),
  session_id       TEXT        NOT NULL CHECK (btrim(session_id) <> ''),
  tool_name        TEXT,
  -- `policy` is what this row actually stored; `declared_policy` is what the
  -- relation asked for. They differ exactly when an over-cap inline payload
  -- degraded to digest, so the degradation is readable and never silent.
  policy           TEXT        NOT NULL
    CHECK (policy IN ('metadata', 'inline', 'digest')),
  declared_policy  TEXT        NOT NULL
    CHECK (declared_policy IN ('metadata', 'inline', 'digest')),
  payload          JSONB,
  payload_sha256   TEXT,
  payload_bytes    INTEGER CHECK (payload_bytes IS NULL OR payload_bytes >= 0),
  summary          TEXT,
  -- Where a digested payload can be fetched again: {query, source}. `query`
  -- carries the call's argument SHAPE, and an argument's VALUE only when the
  -- relation named that key in its refetch_query_keys allowlist (see
  -- okg.substrate.external_backed.call_sample.refetch_pointer_for_call). A
  -- caller's arguments are NOT copied in wholesale: a cold-class row lives for
  -- 730 days, and a credential or a person's name passed as a tool argument
  -- would live here for all of it.
  refetch          JSONB,
  attrs            JSONB       NOT NULL DEFAULT '{}'::jsonb,
  PRIMARY KEY (observed_at, sample_id),
  -- inline never exceeds the 16 KiB cap; over-cap payloads degrade to digest
  -- in the writer.
  CONSTRAINT call_sample_cold_inline_cap CHECK (
    policy <> 'inline' OR payload_bytes IS NULL OR payload_bytes <= 16384
  ),
  -- a digest row stores no payload ...
  CONSTRAINT call_sample_cold_digest_no_payload CHECK (
    policy <> 'digest' OR payload IS NULL
  ),
  -- ... and whenever it records that a payload existed, it must carry the
  -- sha256 and a re-fetch pointer holding query and source.
  CONSTRAINT call_sample_cold_digest_pointer CHECK (
    policy <> 'digest'
    OR payload_bytes IS NULL
    OR (payload_sha256 IS NOT NULL AND refetch ? 'query' AND refetch ? 'source')
  ),
  -- metadata keeps no payload bytes at all.
  CONSTRAINT call_sample_cold_metadata_bare CHECK (
    policy <> 'metadata' OR (payload IS NULL AND payload_sha256 IS NULL)
  )
);

CREATE INDEX IF NOT EXISTS call_sample_cold_node_relation_time_idx
  ON okg.call_sample_cold (deployment_name, node_id, relation, observed_at DESC);
CREATE INDEX IF NOT EXISTS call_sample_cold_session_trace_idx
  ON okg.call_sample_cold (session_id, trace_id);


SELECT create_hypertable(
  'okg.call_sample_hot', 'observed_at',
  chunk_time_interval => INTERVAL '1 hour',
  if_not_exists => TRUE, migrate_data => TRUE
);
SELECT create_hypertable(
  'okg.call_sample_warm', 'observed_at',
  chunk_time_interval => INTERVAL '1 day',
  if_not_exists => TRUE, migrate_data => TRUE
);
SELECT create_hypertable(
  'okg.call_sample_cold', 'observed_at',
  chunk_time_interval => INTERVAL '7 days',
  if_not_exists => TRUE, migrate_data => TRUE
);

ALTER TABLE okg.call_sample_hot SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'deployment_name',
  timescaledb.compress_orderby   = 'observed_at DESC'
);
ALTER TABLE okg.call_sample_warm SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'deployment_name',
  timescaledb.compress_orderby   = 'observed_at DESC'
);
ALTER TABLE okg.call_sample_cold SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'deployment_name',
  timescaledb.compress_orderby   = 'observed_at DESC'
);

SELECT add_compression_policy('okg.call_sample_hot',  INTERVAL '1 day',   if_not_exists => TRUE);
SELECT add_compression_policy('okg.call_sample_warm', INTERVAL '7 days',  if_not_exists => TRUE);
SELECT add_compression_policy('okg.call_sample_cold', INTERVAL '30 days', if_not_exists => TRUE);

SELECT add_retention_policy('okg.call_sample_hot',  INTERVAL '7 days',   if_not_exists => TRUE);
SELECT add_retention_policy('okg.call_sample_warm', INTERVAL '90 days',  if_not_exists => TRUE);
SELECT add_retention_policy('okg.call_sample_cold', INTERVAL '730 days', if_not_exists => TRUE);

-- Per-(node_id, relation, class) earliest-retained watermark, the same
-- mechanism okg.node_series_watermark provides for series reads: it survives
-- chunk eviction so the windowed reader can tell `aged_out` (existed, evicted)
-- from `no_data` (never existed, or a gap inside the retained window).
-- Coercing empty-equals-no_data is forbidden.
CREATE TABLE IF NOT EXISTS okg.call_sample_watermark (
  deployment_name    TEXT        NOT NULL,
  node_id            TEXT        NOT NULL,
  relation           TEXT        NOT NULL,
  retention_class    TEXT        NOT NULL
    CHECK (retention_class IN ('hot', 'warm', 'cold')),
  first_observed_at  TIMESTAMPTZ NOT NULL,
  updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (deployment_name, node_id, relation, retention_class)
);

-- Internal AKMON effective-attrs state. Raw attrs remain source-owned
-- in okg.graph_nodes / okg.graph_edges; this table stores derived,
-- reviewer, proposal, stale, superseded, and rejected observations that
-- project onto flat effective attrs at read time.
CREATE TABLE IF NOT EXISTS okg.attribute_observations (
  observation_id TEXT PRIMARY KEY,
  target_kind TEXT NOT NULL CHECK (target_kind IN ('node', 'edge')),
  target_id TEXT NOT NULL,
  generation_id TEXT NOT NULL,
  namespace TEXT NOT NULL DEFAULT 'default',
  attr_key TEXT NOT NULL,
  value JSONB NOT NULL,
  value_hash TEXT NOT NULL,
  status TEXT NOT NULL
    CHECK (status IN ('candidate', 'accepted', 'rejected',
                      'superseded', 'stale')),
  producer TEXT NOT NULL,
  provenance JSONB NOT NULL DEFAULT '{}'::jsonb,
  dependencies JSONB NOT NULL DEFAULT '{}'::jsonb,
  dependency_fingerprint TEXT,
  supersedes TEXT,
  superseded_by TEXT,
  decision JSONB NOT NULL DEFAULT '{}'::jsonb,
  confidence DOUBLE PRECISION,
  observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_attribute_observations_current_unique
  ON okg.attribute_observations
  (target_kind, target_id, generation_id, namespace, attr_key)
  WHERE status = 'accepted' AND superseded_by IS NULL;

CREATE INDEX IF NOT EXISTS idx_attribute_observations_target
  ON okg.attribute_observations
  (target_kind, target_id, generation_id, namespace, attr_key, status);

CREATE INDEX IF NOT EXISTS idx_attribute_observations_dependency
  ON okg.attribute_observations (dependency_fingerprint)
  WHERE dependency_fingerprint IS NOT NULL;

-- Immutable effective-attribute snapshots keyed by generation. Default
-- MCP reads overlay this table, not mutable observation status, so a
-- pinned session sees the same attrs after later stale/supersede writes.
CREATE TABLE IF NOT EXISTS okg.attribute_effective_attrs (
  target_kind TEXT NOT NULL CHECK (target_kind IN ('node', 'edge')),
  target_id TEXT NOT NULL,
  generation_id TEXT NOT NULL,
  namespace TEXT NOT NULL DEFAULT 'default',
  attr_key TEXT NOT NULL,
  observation_id TEXT NOT NULL,
  value JSONB NOT NULL,
  value_hash TEXT NOT NULL,
  producer TEXT NOT NULL,
  provenance JSONB NOT NULL DEFAULT '{}'::jsonb,
  dependencies JSONB NOT NULL DEFAULT '{}'::jsonb,
  dependency_fingerprint TEXT,
  confidence DOUBLE PRECISION,
  materialized_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (target_kind, target_id, generation_id, namespace, attr_key)
);

CREATE INDEX IF NOT EXISTS idx_attribute_effective_attrs_target
  ON okg.attribute_effective_attrs
  (target_kind, target_id, generation_id, namespace);

CREATE INDEX IF NOT EXISTS idx_attribute_effective_attrs_observation
  ON okg.attribute_effective_attrs (observation_id);

-- schema-interval-narrow-split (req.sigma-pinned): the ONE in-database content
-- hash site. okg.jsonb_canonical_text renders a stored jsonb value
-- deterministically (keys sorted under COLLATE "C", numerics trim_scale'd so
-- jsonb-EQUAL values that differ only in scale render identically);
-- okg.attribute_content_hash is sha256 over canonical(attrs) + a presence
-- marker + canonical(raw_attrs). Mirrors attributes.py _SCHEMA_STATEMENTS and
-- migration 0020_interval_narrow_split byte-for-byte.
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

-- schema-interval-narrow-split (req.split-shape): the insert-once
-- content-addressed payload store. Interval and snapshot rows reference it via
-- a composite FK ON DELETE RESTRICT; presence-means-overlay raw_attrs NULL
-- semantics live here (NULL means raw == attrs; never DEFAULT '{}').
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

-- scale-operator-attr-reads / req.effective-attrs-view (Layer 2): a per-generation
-- MATERIALIZED snapshot of EFFECTIVE node/edge attrs (raw graph attrs merged with
-- the accepted overlay above) carrying exploded stable columns. The MCP attr
-- operators (aggregate/filter/map over attrs.<key>) filter/group/aggregate over
-- this table IN SQL (index scan) so a large subtype -- e.g. okg-ci test_case,
-- hundreds of thousands of rows -- no longer trips the bounded Python overlay cap.
-- DERIVED downstream of attribute_effective_attrs; generation_id is the opaque
-- okg:...:gen:... TEXT string (NOT bigint). Mirrors attributes.py _SCHEMA_STATEMENTS.
-- schema-interval-narrow-split: the row carries attrs_hash (LAST column so
-- migrated deployments converge to the fresh column order); the jsonb payload
-- lives once in okg.attribute_versions.
CREATE TABLE IF NOT EXISTS okg.attribute_effective_view (
  target_kind TEXT NOT NULL CHECK (target_kind IN ('node', 'edge')),
  target_id TEXT NOT NULL,
  generation_id TEXT NOT NULL,
  namespace TEXT NOT NULL DEFAULT 'default',
  subtype TEXT,
  src TEXT,
  dst TEXT,
  edge_type TEXT,
  overlay_count INT NOT NULL DEFAULT 0,
  raw_text_hash TEXT,
  materialized_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  attrs_hash BYTEA NOT NULL,
  PRIMARY KEY (target_kind, target_id, generation_id, namespace),
  CONSTRAINT attribute_effective_view_version_fkey
    FOREIGN KEY (target_kind, target_id, attrs_hash)
    REFERENCES okg.attribute_versions (target_kind, target_id, attrs_hash)
    ON DELETE RESTRICT
);

-- Do NOT add an `ALTER TABLE ... ADD COLUMN IF NOT EXISTS attrs` here to make
-- a legacy database converge. `attrs`/`raw_attrs` deliberately no longer live
-- on this table -- 0020_interval_narrow_split moved them to the
-- content-addressed version store and DROPS both columns from an upgraded
-- table, leaving only `attrs_hash`. Re-adding them in this file would fight
-- that migration on every run and reinstate the wide rows the split exists to
-- remove. (Written down because the natural fix for a legacy `column "attrs"
-- does not exist` abort during migrate is exactly that ALTER, and here it is
-- the wrong one: the index that used to raise it is gone from this file.)

CREATE INDEX IF NOT EXISTS attribute_effective_view_node_subtype_idx
  ON okg.attribute_effective_view
  (generation_id, namespace, subtype, target_id)
  WHERE target_kind = 'node';

CREATE INDEX IF NOT EXISTS attribute_effective_view_edge_type_idx
  ON okg.attribute_effective_view
  (generation_id, namespace, edge_type, target_id)
  WHERE target_kind = 'edge';

-- Roles (app_rw, app_ro, catalog_admin) are created by
-- ops/pg/initdb/00-extensions.sql on fresh compose bring-up, and
-- by okg.substrate.db.migrate on a managed host that doesn't run
-- the initdb. Grants are idempotent; re-running this section
-- converges.

-- app_rw: runtime writer path.
--
-- INVARIANT (M4.5, per Dijkstra review P1 #5): the `app_rw` role has
-- SELECT, INSERT, UPDATE — but NOT DELETE — on the append-only fact and
-- ledger tables below. A published generation must stay readable for as
-- long as it is pinnable, so retraction and supersession are recorded, not
-- erased. The absence of DELETE in this grant is load-bearing; don't add
-- it to one of these tables without reasoning about what a pinned reader
-- at an older generation would then fail to find. (The DELETE grants that
-- do appear further down are on per-generation caches whose lifetime is
-- tied to the checkpoint they belong to, and each says so.)
GRANT USAGE ON SCHEMA okg TO app_rw;
GRANT USAGE ON SCHEMA okg_derived TO app_rw;
GRANT SELECT, INSERT, UPDATE ON
  okg.node_facts, okg.edge_facts,
  okg.graph_generations,
  okg.producer_profiles,
  okg.admission_envelopes,
  okg.fact_batch_receipts,
  okg.schema_migrations,
  okg.source_watermarks,
  okg.link_cache,
  okg.mcp_trace_events,
  okg.link_retry,
  okg.extractor_retry_queue,
  okg.projection_remediation_runs,
  okg.projection_remediation_batches,
  okg.alias_collisions,
  okg.telemetry_traces,
  okg.telemetry_spans,
  okg.telemetry_events,
  okg.telemetry_graph_effect_manifests,
  okg.telemetry_graph_effects,
  okg.telemetry_metrics,
  okg.telemetry_artifacts,
  okg.enricher_progress,
  okg.enricher_backfill_progress,
  okg.source_progress,
  okg.derived_artifact_definitions,
  okg.derived_artifact_runs,
  okg.derived_output_manifests,
  okg.derived_output_slots,
  okg.derived_output_lineage_envelopes,
  okg.derived_output_dependency_index,
  okg.derived_relation_registry,
  okg.derived_dirty_rule_runs,
  okg.derived_dirty_scopes,
  okg.derived_dirty_rule_audits,
  okg.derived_artifact_dependencies,
  okg.derived_schedule_plans,
  okg.derived_schedule_nodes,
  okg.derived_run_dependency_bindings,
  okg.consistency_check_definitions,
  okg.consistency_candidates,
  okg.consistency_candidate_coverage,
  okg.consistency_gate_results,
  okg.nomos_identity_providers,
  okg.nomos_external_claims,
  okg.nomos_subject_bindings,
  okg.nomos_permission_grants,
  okg.nomos_decisions,
  okg.observation_current,
  okg.external_probe_cache,
  okg.node_series_watermark,
  okg.call_sample_watermark,
  okg.attribute_observations
TO app_rw;
-- Graph-effect identity rows are bounded telemetry retention, not pinned graph
-- projection state. Re-recording a manifest replaces its retained page, so the
-- runtime writer needs this narrow DELETE grant for stale identities only.
GRANT DELETE ON okg.telemetry_graph_effects TO app_rw;
GRANT SELECT, INSERT ON
  okg.observation_events,
  okg.admission_events,
  okg.admission_record_refs,
  okg.node_series,
  -- Call samples are append-only: the writer inserts, readers SELECT. No
  -- UPDATE/DELETE grant, so a sample cannot be rewritten after the fact.
  okg.call_sample_hot,
  okg.call_sample_warm,
  okg.call_sample_cold,
  okg.attribute_effective_attrs,
  -- Figure-crop staging: the VisualCaptionEnricher reads crop bytes on the
  -- app_rw publisher connection; the discovery source stages them (also app_rw
  -- where reader and writer share a role). SELECT + INSERT covers both.
  okg.visual_crop_blobs
TO app_rw;
-- scale-operator-attr-reads / req.effective-attrs-view (Layer 2): the attr
-- operators read this table on the app_rw MCP pool; the publisher INSERTs the
-- per-generation snapshot; delete_generation DELETEs it in lockstep with the
-- checkpoint (the snapshot is keyed by generation_id and dies with that
-- generation, so it is not covered by the DELETE-absence note above; DELETE
-- here is safe and required to avoid unbounded snapshot growth).
GRANT SELECT, INSERT, DELETE ON
  okg.attribute_effective_view
TO app_rw;
-- link_retry: the Publisher INSERTs resolver misses; alias landing marks them
-- ready, and the Publisher DELETEs exact receipts only after publication. The
-- compatibility Projector uses the same table. alias_collisions: the app_rw
-- read path SELECTs standing collisions; DELETE clears stale ones.
GRANT SELECT, INSERT, UPDATE, DELETE ON
  okg.link_retry,
  okg.extractor_retry_queue,
  okg.alias_collisions,
  -- Caption-retry quarantine: the enricher SELECTs the counts, upserts
  -- (INSERT/UPDATE) a failure increment, and DELETEs the row on success.
  okg.visual_caption_failures,
  -- Shared extraction retry/quarantine ledger: the publisher claims,
  -- increments, resolves, and surfaces active rows through doctor.
  okg.enricher_input_quarantines,
  -- General blob store: ingest inserts and refreshes rows (last_used_at),
  -- and `okg db prune-blobs` deletes unreferenced ones.
  okg.blobs
TO app_rw;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA okg TO app_rw;

GRANT SELECT, INSERT ON
  okg.workflow_operational_registrations,
  okg.workflow_operational_outcomes
TO app_rw;
REVOKE UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON
  okg.workflow_operational_registrations,
  okg.workflow_operational_outcomes
FROM app_rw;
REVOKE CREATE ON SCHEMA okg, okg_derived FROM app_rw;
-- Convergence guard for upgraded databases: older snapshots granted app_rw
-- broader catalog writes. The current contract is runtime-writer only; catalog
-- mutation belongs to catalog_admin. Revoke stale writes before granting.
REVOKE INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON
  okg.catalog_versions,
  okg.runtime_dlq_acknowledgement_events
FROM app_rw;
GRANT SELECT ON
  okg.subtypes, okg.narrowings, okg.aliases, okg.invariants,
  okg.identifier_patterns,
  okg.catalog_versions,
  okg.runtime_dlq_acknowledgement_events
TO app_rw;

-- app_ro: MCP server read path.
--
-- Per `harden-mcp-run-sql-safety` (2026-04-27): all `app_ro`
-- privilege management lives in views.sql under a default-deny
-- pattern. schema.sql deliberately does NOT grant `app_ro` any
-- table-level access — schema-defining migrations no longer need
-- to remember which tables to revoke. New tables in schema okg
-- default to inaccessible; the only way to expose data to
-- `app_ro` is to add a corresponding `okg.v_*` view in views.sql.
--
-- (`app_ro` was previously granted SELECT on 14 tables here and
-- those grants were partially revoked in views.sql:258-266 — a
-- spec/implementation contradiction the design review caught.)

-- Both fuzzy + identifier SQL functions are STABLE/PARALLEL SAFE
-- and used by the privileged MCP-server connection (which runs
-- as `app_rw` for substrate-side tools).
GRANT EXECUTE ON FUNCTION
  okg.find_identifier_mentions(TEXT, BIGINT),
  okg.fuzzy_resolve_alias(TEXT, BIGINT, REAL, INTEGER)
TO app_rw;

-- catalog_admin: ontology loader.
GRANT USAGE ON SCHEMA okg TO catalog_admin;
GRANT SELECT, INSERT, UPDATE ON
  okg.subtypes, okg.narrowings, okg.aliases, okg.invariants,
  okg.identifier_patterns,
  okg.catalog_versions
TO catalog_admin;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON
  okg.runtime_dlq_acknowledgement_events
FROM catalog_admin;
GRANT SELECT ON
  okg.runtime_dlq_acknowledgement_events
TO catalog_admin;
-- Alias seeding runs at catalog-apply time (catalog_admin) and refuses
-- colliding values, persisting/refreshing okg.alias_collisions; the landing
-- hook marks matching okg.link_retry rows ready for Publisher processing.
GRANT SELECT, INSERT, UPDATE, DELETE ON
  okg.aliases,
  okg.alias_collisions,
  okg.link_cache,
  okg.link_retry,
  okg.extractor_retry_queue,
  okg.projection_remediation_runs,
  okg.projection_remediation_batches
TO catalog_admin;
GRANT USAGE, SELECT ON
  okg.catalog_versions_catalog_version_id_seq
TO catalog_admin;

-- Bounded historical repair needs the union of runtime-writer and catalog
-- quarantine capabilities, but not database ownership. Keep the existing
-- roles narrow and expose that union through one explicit NOLOGIN boundary.
GRANT app_rw, catalog_admin TO projection_remediator;
-- Every fact write stamps the active Nomos policy fingerprint
-- (okg.substrate.fact_application -> nomos.store.load_active_policy_bundle),
-- so a remediation that writes facts must be able to READ the active policy.
-- Read only, as okg_mcp already has (PACT remediator-nomos-policy-read).
GRANT SELECT ON okg.nomos_policy_bundles, okg.nomos_policy_activations
  TO projection_remediator;

-- ========== LLM call tracking (per add-substrate-llm-call-tracking) ==========
--
-- Single source of truth for LLM API spend across the substrate.
-- Every LiteLLMClient.complete(...) call records one row here
-- with the provider-reported token counts and cost (when
-- available — OpenRouter returns usage.cost reliably; other
-- providers may leave fields NULL until their handler is wired).
--
-- The companion okg.llm_call_stats view groups the table by
-- caller + model + day for spend roll-ups via
-- `okg metrics --llm`.
--
-- Caller string convention: '<kind>:<id>' where kind is one of
-- views | invariants | recheck | concept_extraction | verifier
-- | viz_chat | other.

CREATE TABLE IF NOT EXISTS okg.llm_calls (
  id                BIGSERIAL        PRIMARY KEY,
  ts                TIMESTAMPTZ      NOT NULL DEFAULT now(),
  deployment_name        TEXT,
  caller            TEXT             NOT NULL,
  model             TEXT             NOT NULL,
  fragment_id       TEXT,
  prompt_tokens     INT,
  completion_tokens INT,
  total_tokens      INT,
  cost_usd          NUMERIC(12, 8),
  latency_ms        INT,
  success           BOOLEAN          NOT NULL DEFAULT TRUE,
  error_kind        TEXT,
  generation_id     TEXT COLLATE "C",
  extras            JSONB
);

CREATE INDEX IF NOT EXISTS llm_calls_ts_idx
  ON okg.llm_calls (ts DESC);

-- Guarded for the upgrade path (schema-identity-convergence, migration
-- 0026): schema.sql runs before the ordered migrations, and a
-- CREATE INDEX IF NOT EXISTS resolves its column list BEFORE checking the
-- index name, so naming the renamed column unguarded would abort the
-- migrate on a database 0026 has not reached yet. On upgrade the guard
-- declines and 0026's RENAME COLUMN carries the existing index over.
DO $guard_llm_calls_deployment_caller_ts_idx$
BEGIN
  IF EXISTS (
    SELECT 1
      FROM pg_attribute
     WHERE attrelid = 'okg.llm_calls'::regclass
       AND attname = 'deployment_name'
       AND attnum > 0
       AND NOT attisdropped
  ) THEN
    EXECUTE $idx$
      CREATE INDEX IF NOT EXISTS llm_calls_deployment_caller_ts_idx
        ON okg.llm_calls (deployment_name, caller, ts DESC)
    $idx$;
  END IF;
END
$guard_llm_calls_deployment_caller_ts_idx$;

GRANT SELECT, INSERT ON okg.llm_calls TO app_rw;

-- Guarded on the renamed column, and this is the ONE place in the snapshot
-- that needs such a guard (schema-identity-convergence, migration 0026).
--
-- schema.sql executes BEFORE the ordered migrations (migrate.py:1059 then
-- :1092), so on an UPGRADED database this statement runs while okg.llm_calls
-- still has the old bare `deployment` column. `CREATE TABLE IF NOT EXISTS` and
-- `CREATE INDEX IF NOT EXISTS` are no-ops there -- an existing object name
-- means the body is never parsed -- but `CREATE OR REPLACE VIEW` re-executes
-- its body every time, so an unguarded definition aborts the whole migrate
-- with `column "deployment_name" does not exist` before 0026 can rename
-- anything.
--
-- The guard is not a silent fallback: when it declines, the pre-existing view
-- is left exactly as it was and migration 0026 re-creates it against the
-- renamed column in the same transaction, so the upgraded database ends the
-- migrate with the same view shape a fresh install gets. The convergence lane
-- is what proves that, rather than this comment.
DO $llm_call_stats_view$
BEGIN
  IF EXISTS (
    SELECT 1
      FROM pg_attribute
     WHERE attrelid = 'okg.llm_calls'::regclass
       AND attname = 'deployment_name'
       AND attnum > 0
       AND NOT attisdropped
  ) THEN
    EXECUTE $view$
      CREATE OR REPLACE VIEW okg.llm_call_stats AS
        SELECT deployment_name,
               caller,
               model,
               date_trunc('day', ts) AS day,
               COUNT(*)               AS calls,
               SUM(prompt_tokens)     AS prompt_tokens,
               SUM(completion_tokens) AS completion_tokens,
               SUM(cost_usd)          AS cost_usd
          FROM okg.llm_calls
         WHERE success
         GROUP BY 1, 2, 3, 4
    $view$;
  END IF;
END
$llm_call_stats_view$;

GRANT SELECT ON okg.llm_call_stats TO app_rw;

-- Stable outcome-key digest per model-backed invocation (spec §9.2,
-- consistency-model-outcome-replay). Additive; NULL for legacy rows and
-- for call paths recorded before their site computed keys.
ALTER TABLE okg.llm_calls
  ADD COLUMN IF NOT EXISTS outcome_key_digest TEXT;

-- ========== Keyed model outcomes (omega) — okg.model_outcomes ==========
--
-- One retained outcome per (deployment, OutcomeKey digest): the content
-- the pipeline consumed at the client boundary, recorded first-write-wins
-- so a later fresh sample under the same key never rewrites history.
-- Retention is manifest-gated and bounded (OutcomeStore.max_rows,
-- oldest-first pruning at write); replay reads are exact-digest only.
-- Spec: docs/develop/architecture/consistency.md §9.2; identifiers pinned
-- by the ratified notation map.

CREATE TABLE IF NOT EXISTS okg.model_outcomes (
  deployment_name        TEXT        NOT NULL,
  key_digest        TEXT        NOT NULL,
  step_id           TEXT        NOT NULL,
  operator_version  TEXT        NOT NULL,
  model_id          TEXT        NOT NULL,
  prompt_hash       TEXT        NOT NULL,
  input_hash        TEXT        NOT NULL,
  content           TEXT        NOT NULL,
  recorded_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  generation_id     TEXT,
  PRIMARY KEY (deployment_name, key_digest)
);

-- Guarded for the upgrade path (schema-identity-convergence, migration
-- 0026): schema.sql runs before the ordered migrations, and a
-- CREATE INDEX IF NOT EXISTS resolves its column list BEFORE checking the
-- index name, so naming the renamed column unguarded would abort the
-- migrate on a database 0026 has not reached yet. On upgrade the guard
-- declines and 0026's RENAME COLUMN carries the existing index over.
DO $guard_model_outcomes_recorded_at_idx$
BEGIN
  IF EXISTS (
    SELECT 1
      FROM pg_attribute
     WHERE attrelid = 'okg.model_outcomes'::regclass
       AND attname = 'deployment_name'
       AND attnum > 0
       AND NOT attisdropped
  ) THEN
    EXECUTE $idx$
      CREATE INDEX IF NOT EXISTS model_outcomes_recorded_at_idx
        ON okg.model_outcomes (deployment_name, recorded_at DESC)
    $idx$;
  END IF;
END
$guard_model_outcomes_recorded_at_idx$;

GRANT SELECT, INSERT, DELETE ON okg.model_outcomes TO app_rw;


-- ========== Envelope violations — okg.envelope_violations ==========
--
-- One row per well-typed output rejected outside its producer's declared
-- write envelope (spec §9.3, consistency-output-envelopes). The row is the
-- reuse-poisoning substrate: certificate validation must refuse any
-- certificate whose producer has a violation at or after the certificate's
-- recorded generation. Violations surface in the publish-cycle outcome;
-- nothing here ever widens an envelope.

CREATE TABLE IF NOT EXISTS okg.envelope_violations (
  id             BIGSERIAL   PRIMARY KEY,
  deployment_name     TEXT        NOT NULL,
  generation_id  TEXT,
  producer_id    TEXT        NOT NULL,
  code           TEXT        NOT NULL,
  record_kind    TEXT,
  detail         JSONB       NOT NULL,
  poisons_reuse  BOOLEAN     NOT NULL DEFAULT TRUE,
  recorded_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Guarded for the upgrade path (schema-identity-convergence, migration
-- 0026): schema.sql runs before the ordered migrations, and a
-- CREATE INDEX IF NOT EXISTS resolves its column list BEFORE checking the
-- index name, so naming the renamed column unguarded would abort the
-- migrate on a database 0026 has not reached yet. On upgrade the guard
-- declines and 0026's RENAME COLUMN carries the existing index over.
DO $guard_envelope_violations_deployment_generation_idx$
BEGIN
  IF EXISTS (
    SELECT 1
      FROM pg_attribute
     WHERE attrelid = 'okg.envelope_violations'::regclass
       AND attname = 'deployment_name'
       AND attnum > 0
       AND NOT attisdropped
  ) THEN
    EXECUTE $idx$
      CREATE INDEX IF NOT EXISTS envelope_violations_deployment_generation_idx
        ON okg.envelope_violations (deployment_name, generation_id)
    $idx$;
  END IF;
END
$guard_envelope_violations_deployment_generation_idx$;

GRANT SELECT, INSERT ON okg.envelope_violations TO app_rw;


-- ========== Repair selections (r*) — okg.repair_selections ==========
--
-- One row per (deployment, generation_id, bucket) recorded repair selection:
-- the reified r* the claim_projection derivation materializes on EVERY
-- generation. Selection semantics are INHERITED from the pure layer
-- (okg.substrate.consistency.claim_model.selection.select_repairs) — this
-- table RECORDS the edit set; it never re-derives it. A decisively resolved
-- bucket records outcome='selected' with the precedence-retraction generator,
-- the winner, the distrusted (superseded) claim ids, and — in
-- override_claim_ids — the second repair the bucket had available and did not
-- take; a contested bucket
-- records outcome='unresolved' with reason contested (the empty repair — the
-- engine's ranking-only-never-settles abstention, spec §5). Source
-- preservation lives in okg.claim_projection (losers stay present as
-- superseded, never deleted); these rows only record which claims the
-- selected repair distrusts. selection_digest is the byte-stable sha256 of
-- the whole-generation SelectedRepair (order-independent), so two generations'
-- selections compare byte-stably and a generation's r* is recoverable from
-- its rows. generation_id is the compat generation token
-- (out["compat_generation_id"]).
--
-- The claim plane is DIFFERENTIAL (§9.8): a bucket-scoped incremental cycle
-- re-selects only the DIRTY buckets through the pure layer and copies every
-- untouched bucket's selection row forward from the prior generation, so every
-- generation carries a COMPLETE selection set and the incremental and
-- from-scratch paths produce identical selections. carried_from_generation
-- records that carry provenance honestly: for a copied-forward row it is the
-- prior generation_id the row was carried from; it is NULL for a freshly
-- selected row (every row of a full recompute). It is maintenance provenance,
-- not selection content — the equivalence differ does not read it. Spec:
-- docs/develop/architecture/consistency.md §5 (resolution outcomes),
-- §9.8 (differential claim plane); PACT consistency-repair-objective.

CREATE TABLE IF NOT EXISTS okg.repair_selections (
  deployment_name              TEXT        NOT NULL,
  generation_id           TEXT        NOT NULL,
  bucket_key              TEXT        NOT NULL,
  outcome                 TEXT        NOT NULL,   -- selected | unresolved
  winner_claim_id         TEXT,
  generator_id            TEXT,
  generator_version       TEXT,
  basis                   TEXT,
  distrusted_claim_ids    JSONB       NOT NULL DEFAULT '[]'::jsonb,
  asserted_claim_ids      JSONB       NOT NULL DEFAULT '[]'::jsonb,
  unresolved_reason       TEXT,
  carried_from_generation TEXT,       -- prior generation a differential carry
                                      -- copied this row from; NULL when the row
                                      -- was freshly selected this generation
  selection_digest        TEXT        NOT NULL,
  recorded_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
  -- COLUMN ORDER IS LOAD-BEARING BELOW THIS LINE. These two post-date the
  -- pinned dump that test_restored_pinned_dump_migrates_to_fresh_shape
  -- restores, so on an existing database they arrive via the ALTER ... ADD
  -- COLUMN statements below, and ADD COLUMN always APPENDS. Declaring them
  -- mid-table would give a fresh install a different column ORDER from a
  -- migrated one, and that lane compares normalized schema dumps. Keep new
  -- columns at the END, in the same order as their ALTER statements, until a
  -- later dump pin absorbs them.
  override_claim_ids      JSONB       NOT NULL DEFAULT '[]'::jsonb,
  selection_schema_version TEXT       NOT NULL
                            DEFAULT 'okg.claim_model.selection.v1',
  PRIMARY KEY (deployment_name, generation_id, bucket_key)
);

-- Backfill the differential carry-provenance column onto existing tables
-- (task.repair.differential); nullable, so a legacy row reads as "not carried".
ALTER TABLE okg.repair_selections
  ADD COLUMN IF NOT EXISTS carried_from_generation TEXT;

-- Backfill the second-repair column onto existing tables (PACT
-- concord-second-repair-generator). override_claim_ids is CANDIDATE-SET
-- content, not the selected edit: the claims the override generator would have
-- rewritten to winner_claim_id's value had that repair been chosen, and empty
-- when it refused (a contested bucket, a bucket carrying no values, or a typed
-- bucket whose ranking "value" is a surrogate node id). It makes "which repairs
-- did this generation choose between" answerable from the published rows; it is
-- also the pair (targets + winner) the differential carry path needs to rebuild
-- the whole-generation selection_digest from rows alone. Defaults to '[]', so a
-- legacy row reads as "no second repair was recorded".
ALTER TABLE okg.repair_selections
  ADD COLUMN IF NOT EXISTS override_claim_ids JSONB NOT NULL DEFAULT '[]'::jsonb;

-- The UPGRADE FENCE for the differential carry. '[]' above is a DDL default,
-- not a measurement: a row written before this PACT records "no override was
-- computed", which is INDISTINGUISHABLE in the column from the override
-- generator legitimately refusing (a contested bucket, a value-less bucket, a
-- surrogate-valued bucket). So the column alone cannot be backfilled — instead
-- every row records the selection schema version it was written under, and the
-- differential carry copies a prior row forward only when that version equals
-- the running SELECTION_SCHEMA_VERSION (claim_model/selection.py). A legacy row
-- takes this column's default (v1), mismatches the running v2, and the
-- claim_projection derivation falls back to a FULL recompute for that cycle
-- (recorded cause 'selection_schema_upgrade') — so the first cycle after an
-- upgrade rewrites the whole selection set rather than carrying rows whose
-- candidate set is missing the override candidate and whose whole-generation
-- selection_digest would no longer match a from-scratch recompute.
ALTER TABLE okg.repair_selections
  ADD COLUMN IF NOT EXISTS selection_schema_version TEXT NOT NULL
    DEFAULT 'okg.claim_model.selection.v1';

-- Guarded for the upgrade path (schema-identity-convergence, migration
-- 0026): schema.sql runs before the ordered migrations, and a
-- CREATE INDEX IF NOT EXISTS resolves its column list BEFORE checking the
-- index name, so naming the renamed column unguarded would abort the
-- migrate on a database 0026 has not reached yet. On upgrade the guard
-- declines and 0026's RENAME COLUMN carries the existing index over.
DO $guard_repair_selections_deployment_generation_idx$
BEGIN
  IF EXISTS (
    SELECT 1
      FROM pg_attribute
     WHERE attrelid = 'okg.repair_selections'::regclass
       AND attname = 'deployment_name'
       AND attnum > 0
       AND NOT attisdropped
  ) THEN
    EXECUTE $idx$
      CREATE INDEX IF NOT EXISTS repair_selections_deployment_generation_idx
        ON okg.repair_selections (deployment_name, generation_id)
    $idx$;
  END IF;
END
$guard_repair_selections_deployment_generation_idx$;

GRANT SELECT, INSERT, DELETE ON okg.repair_selections TO app_rw;


-- ========== Reuse certificates — okg.reuse_certificates ==========
--
-- One row per skipped step inside the resolved state's dependency cone: the
-- persisted justification a differential update records when it KEEPS a prior
-- result instead of re-running the step (spec §9.5, PACT
-- consistency-resolution-aware-reuse). Append-only, keyed
-- (deployment, generation_id, step_id).
--
-- Each row records (a) the COMPARISON that justified reuse -- validated against
-- its declared `kind`: the effects-backed `resolution_dominance` schema (the
-- incumbent decision's worst outcome against competitors' best under every
-- permitted output, tie-break included --
-- okg.substrate.consistency.reuse_certificates.REQUIRED_COMPARISON_KEYS) or the
-- `degenerate_unchanged_inputs` schema (DEGENERATE_COMPARISON_KEYS) for the
-- unchanged-inputs carry-forward, whose justification is that every recorded
-- dependency is byte-identical to the prior generation's -- a JSONB object,
-- and (b) the DEPENDENCIES that comparison consumed -- a typed per-class JSONB
-- mapping (input digests, operator/model/prompt versions, envelope + catalog
-- fingerprints, constraint-set checksums, generator versions, policy /
-- precedence fingerprints, competitor bounds). certificate_digest is the
-- byte-stable sha256 of (step_id, comparison, dependencies) -- the deployment /
-- generation labels are the row key, not justification content, so a degenerate
-- carry-forward certificate is byte-stable across generations.
--
-- There is NO status / invalidated_at column: validity is EVALUATED, never
-- stored. validate_certificate() compares each recorded dependency class
-- against the current facts and returns exactly the invalidated classes; a
-- stored flag would be a second source of truth that could drift from the
-- facts. Recording a skip without a certificate is structurally impossible --
-- the store's record_skip takes a ReuseCertificate, not an optional. Spec:
-- docs/develop/architecture/consistency.md §9.5.

CREATE TABLE IF NOT EXISTS okg.reuse_certificates (
  deployment_name          TEXT        NOT NULL,
  generation_id       TEXT        NOT NULL,
  step_id             TEXT        NOT NULL,
  certificate_digest  TEXT        NOT NULL,
  comparison          JSONB       NOT NULL,
  dependencies        JSONB       NOT NULL,
  recorded_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (deployment_name, generation_id, step_id),
  CONSTRAINT reuse_certificates_digest_nonempty
    CHECK (certificate_digest <> ''),
  CONSTRAINT reuse_certificates_comparison_object
    CHECK (jsonb_typeof(comparison) = 'object'),
  CONSTRAINT reuse_certificates_dependencies_object
    CHECK (jsonb_typeof(dependencies) = 'object')
);

-- Guarded for the upgrade path (schema-identity-convergence, migration
-- 0026): schema.sql runs before the ordered migrations, and a
-- CREATE INDEX IF NOT EXISTS resolves its column list BEFORE checking the
-- index name, so naming the renamed column unguarded would abort the
-- migrate on a database 0026 has not reached yet. On upgrade the guard
-- declines and 0026's RENAME COLUMN carries the existing index over.
DO $guard_reuse_certificates_deployment_generation_idx$
BEGIN
  IF EXISTS (
    SELECT 1
      FROM pg_attribute
     WHERE attrelid = 'okg.reuse_certificates'::regclass
       AND attname = 'deployment_name'
       AND attnum > 0
       AND NOT attisdropped
  ) THEN
    EXECUTE $idx$
      CREATE INDEX IF NOT EXISTS reuse_certificates_deployment_generation_idx
        ON okg.reuse_certificates (deployment_name, generation_id)
    $idx$;
  END IF;
END
$guard_reuse_certificates_deployment_generation_idx$;

-- Guarded for the upgrade path (schema-identity-convergence, migration
-- 0026): schema.sql runs before the ordered migrations, and a
-- CREATE INDEX IF NOT EXISTS resolves its column list BEFORE checking the
-- index name, so naming the renamed column unguarded would abort the
-- migrate on a database 0026 has not reached yet. On upgrade the guard
-- declines and 0026's RENAME COLUMN carries the existing index over.
DO $guard_reuse_certificates_deployment_step_idx$
BEGIN
  IF EXISTS (
    SELECT 1
      FROM pg_attribute
     WHERE attrelid = 'okg.reuse_certificates'::regclass
       AND attname = 'deployment_name'
       AND attnum > 0
       AND NOT attisdropped
  ) THEN
    EXECUTE $idx$
      CREATE INDEX IF NOT EXISTS reuse_certificates_deployment_step_idx
        ON okg.reuse_certificates (deployment_name, step_id)
    $idx$;
  END IF;
END
$guard_reuse_certificates_deployment_step_idx$;

GRANT SELECT, INSERT, DELETE ON okg.reuse_certificates TO app_rw;


-- ========== Equivalence verdicts — okg.equivalence_verdicts ==========
--
-- The persisted per-update, per-comparison verdict of the three-way equivalence
-- harness (PACT consistency-complete-equivalence, task.harness.live-run;
-- req.harness.live-series). For each recorded update in a series
-- (okg.substrate.qa.equivalence_series.run_equivalence_series) the harness
-- renders TWO verdicts through the single equivalence differ
-- (okg.substrate.consistency.equivalence_differ.diff_resolved_states): the
-- differential (production) path vs the complete reference, and the dependency
-- baseline vs the complete reference. Each verdict is EXACTLY one of
-- equal / mismatch / excluded — there is no tolerance tier (spec §9.1); an
-- update whose replay cannot resolve every model-outcome key is EXCLUDED with
-- the missing keys named and is NEVER counted as agreement (spec §9.7).
--
-- release_blocking is STRUCTURAL, not prose: the CHECK ties it to
-- verdict = 'mismatch', so a mismatch in the real series is a release-blocking
-- finding for the reuse machinery BY CONSTRUCTION (an equal / excluded row can
-- never be marked blocking, and a mismatch can never be marked non-blocking).
-- artifact carries the machine-readable named-divergence artifact for a mismatch
-- (every divergent object, per family) or the named exclusion keys for an
-- excluded verdict; it is NULL for an equal verdict. work_summary records the
-- per-step modes / causes + coverage each run already carries (the full
-- charged-work ledger is consistency-maintenance-accounting's task; this stores
-- what exists honestly, invents nothing). production_generation is the compat
-- generation token (out["compat_generation_id"]); reference_ref is the complete
-- reference's compat generation. Spec:
-- docs/develop/architecture/consistency.md §9.1 (same-result contract),
-- §9.7 (complete reference / named exclusions).

CREATE TABLE IF NOT EXISTS okg.equivalence_verdicts (
  deployment_name            TEXT        NOT NULL,
  update_index          INTEGER     NOT NULL,
  production_generation TEXT        NOT NULL,
  reference_ref         TEXT        NOT NULL,
  comparison            TEXT        NOT NULL,   -- differential_vs_reference | baseline_vs_reference
  verdict               TEXT        NOT NULL,   -- equal | mismatch | excluded
  release_blocking      BOOLEAN     NOT NULL,
  artifact              JSONB,
  work_summary          JSONB,
  -- The §9.7 EQUAL-with-certified-differences facet: a JSON array of the
  -- resolved-state divergences a VALID reuse certificate LICENSED (per object,
  -- plus the covering certificate digests). NULL for a plain equal / mismatch /
  -- excluded verdict; non-null ONLY on an `equal` row, so a certified-equal is
  -- never silently indistinguishable from a plain-equal (a certified difference
  -- is a proof, not a tolerance — spec §9.7).
  certified_differences JSONB,
  recorded_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (deployment_name, production_generation, comparison),
  CONSTRAINT equivalence_verdicts_comparison_ck
    CHECK (comparison IN ('differential_vs_reference', 'baseline_vs_reference')),
  CONSTRAINT equivalence_verdicts_verdict_ck
    CHECK (verdict IN ('equal', 'mismatch', 'excluded')),
  -- release_blocking is TRUE iff the verdict is a mismatch — structural, not
  -- prose (spec §9.1: a mismatch is a release-blocking finding, not a statistic).
  CONSTRAINT equivalence_verdicts_release_blocking_ck
    CHECK (release_blocking = (verdict = 'mismatch')),
  -- certified_differences rides ONLY on an equal verdict (§9.7: a mismatch /
  -- excluded update licenses nothing).
  CONSTRAINT equivalence_verdicts_certified_differences_ck
    CHECK (certified_differences IS NULL OR verdict = 'equal')
);
-- Idempotent column add for a schema applied over a pre-facet equivalence_verdicts
-- (CREATE TABLE IF NOT EXISTS is a no-op on an existing table).
ALTER TABLE okg.equivalence_verdicts
  ADD COLUMN IF NOT EXISTS certified_differences JSONB;

-- Guarded for the upgrade path (schema-identity-convergence, migration
-- 0026): schema.sql runs before the ordered migrations, and a
-- CREATE INDEX IF NOT EXISTS resolves its column list BEFORE checking the
-- index name, so naming the renamed column unguarded would abort the
-- migrate on a database 0026 has not reached yet. On upgrade the guard
-- declines and 0026's RENAME COLUMN carries the existing index over.
DO $guard_equivalence_verdicts_deployment_generation_idx$
BEGIN
  IF EXISTS (
    SELECT 1
      FROM pg_attribute
     WHERE attrelid = 'okg.equivalence_verdicts'::regclass
       AND attname = 'deployment_name'
       AND attnum > 0
       AND NOT attisdropped
  ) THEN
    EXECUTE $idx$
      CREATE INDEX IF NOT EXISTS equivalence_verdicts_deployment_generation_idx
        ON okg.equivalence_verdicts (deployment_name, production_generation)
    $idx$;
  END IF;
END
$guard_equivalence_verdicts_deployment_generation_idx$;

GRANT SELECT, INSERT, DELETE ON okg.equivalence_verdicts TO app_rw;


-- ========== Charged maintenance-work ledger — okg.work_ledger ==========
--
-- One row per (deployment, generation_id, mode, category): the charged work a
-- publish / update paid in each adjudicated category (PACT
-- consistency-maintenance-accounting, task.accounting.ledger; spec §9.9). The
-- six categories a savings claim must pay for are effect_analysis,
-- certificate_validation, deterministic_execution, model_calls, propagation,
-- and repair_solving. `generation_id` is the ANCHOR production / compat
-- generation token (out["compat_generation_id"]); a from-scratch VALIDATION run
-- (complete reference / dependency baseline) is keyed to the update's production
-- generation it is the counterfactual FOR, disambiguated by `mode` (its own
-- scratch generation lives in `detail`) — exactly as equivalence_verdicts keys
-- reference runs by production_generation, and so two updates' reference runs
-- (each a fresh scratch DB whose compat id restarts at a low integer) never
-- collide.
--
-- `mode` is differential | complete_reference | dependency_baseline. `validation`
-- is TRUE iff mode <> 'differential' — a DB CHECK ties them so a from-scratch
-- validation run can NEVER be miscounted as production work saved (the savings
-- report sums production charged work over validation = FALSE only). This mirrors
-- the equivalence_verdicts release_blocking CHECK: structural, not prose.
--
-- `measured` is the LOUD-GAP flag. A category the run did NOT instrument records
-- measured = FALSE with NULL work_units — a visible gap in the row, never a
-- silent zero (a silent zero lets an unaccounted category read as "no work",
-- exactly how a net-negative mechanism would hide as a win). A CHECK enforces
-- measured = (work_units IS NOT NULL): a measured category MUST carry a count (an
-- honest zero is allowed), an unmeasured one MUST NOT.
--
-- The model_calls category's token / cost columns are joined from okg.llm_calls
-- by generation (the llm_call_stats idiom) at write time and frozen into the row,
-- so the ledger renders a savings report from its own rows alone — a scratch DB
-- dropped after a validation run does not take its charged model spend with it.
-- `cause_counts` carries the update's closed-taxonomy FallbackCause histogram
-- (src/okg/substrate/telemetry/fallback_taxonomy.py), non-null on the
-- effect_analysis row (the category that owns the scheduling decisions). Spec:
-- docs/develop/architecture/consistency.md §9.9.

CREATE TABLE IF NOT EXISTS okg.work_ledger (
  deployment_name        TEXT        NOT NULL,
  -- COLLATE "C" (D2): the same collation okg.llm_calls.generation_id carries,
  -- so the documented cost join compares memcmp-identical values.
  generation_id     TEXT COLLATE "C" NOT NULL,
  mode              TEXT        NOT NULL DEFAULT 'differential',
  category          TEXT        NOT NULL,
  validation        BOOLEAN     NOT NULL DEFAULT FALSE,
  measured          BOOLEAN     NOT NULL DEFAULT TRUE,
  work_units        BIGINT,
  model_calls       INTEGER,
  prompt_tokens     BIGINT,
  completion_tokens BIGINT,
  total_tokens      BIGINT,
  cost_usd          NUMERIC(18, 8),
  cause_counts      JSONB,
  detail            JSONB       NOT NULL DEFAULT '{}'::jsonb,
  recorded_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (deployment_name, generation_id, mode, category),
  CONSTRAINT work_ledger_category_ck
    CHECK (category IN ('effect_analysis', 'certificate_validation',
                        'deterministic_execution', 'model_calls',
                        'propagation', 'repair_solving')),
  CONSTRAINT work_ledger_mode_ck
    CHECK (mode IN ('differential', 'complete_reference',
                    'dependency_baseline')),
  -- validation is TRUE iff the run is a from-scratch harness validation mode —
  -- structural, so a validation run is never counted as production savings.
  CONSTRAINT work_ledger_validation_ck
    CHECK (validation = (mode <> 'differential')),
  -- LOUD GAP: an unmeasured category records measured = FALSE + NULL work_units,
  -- never a silent zero; a measured category MUST carry a work_units count.
  CONSTRAINT work_ledger_measured_units_ck
    CHECK (measured = (work_units IS NOT NULL))
);

-- Guarded for the upgrade path (schema-identity-convergence, migration
-- 0026): schema.sql runs before the ordered migrations, and a
-- CREATE INDEX IF NOT EXISTS resolves its column list BEFORE checking the
-- index name, so naming the renamed column unguarded would abort the
-- migrate on a database 0026 has not reached yet. On upgrade the guard
-- declines and 0026's RENAME COLUMN carries the existing index over.
DO $guard_work_ledger_deployment_generation_idx$
BEGIN
  IF EXISTS (
    SELECT 1
      FROM pg_attribute
     WHERE attrelid = 'okg.work_ledger'::regclass
       AND attname = 'deployment_name'
       AND attnum > 0
       AND NOT attisdropped
  ) THEN
    EXECUTE $idx$
      CREATE INDEX IF NOT EXISTS work_ledger_deployment_generation_idx
        ON okg.work_ledger (deployment_name, generation_id)
    $idx$;
  END IF;
END
$guard_work_ledger_deployment_generation_idx$;

GRANT SELECT, INSERT, DELETE ON okg.work_ledger TO app_rw;


-- ========== Narrowing enforcement trigger: stub retired ==========
--
-- No DB-trigger skeleton is left here, and no narrowing trigger either. The
-- CONSTRAINT TRIGGER this block once marked "M5 — TODO" was installed on
-- `okg.edges_live`, which `schema-canonical-read-path` deleted along with the
-- rest of the legacy live complex; the `edge_facts` variant the stub sketched
-- had already been superseded and dropped. Narrowing is enforced at publish by
-- the gate (`narrowing_violation` in error_codes.yaml), and the producer write
-- envelope by the single-layer application-path runtime validator (PACT
-- consistency-output-envelopes) — not by a DB trigger on either side.

-- ========== IR programs — okg.ir_programs ==========
--
-- Program content is content-addressed; owner-specific roles and
-- generation/branch execution attempts live in separate tables.
CREATE TABLE IF NOT EXISTS okg.ir_programs (
  program_id            UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  program_checksum      TEXT        NOT NULL UNIQUE,
  ir_version            TEXT        NOT NULL DEFAULT 'okg-ir-v1',
  program_kind          TEXT        NOT NULL
    CHECK (program_kind IN (
      'read', 'write', 'analysis', 'compiler_input',
      'gate', 'lens', 'source', 'projection', 'replay', 'rollback',
      'transform'
    )),
  registry_id           TEXT,
  registry_version      TEXT,
  registry_checksum     TEXT,
  effect_summary        JSONB       NOT NULL DEFAULT '[]'::jsonb,
  determinism           TEXT        NOT NULL,
  op_counts             JSONB       NOT NULL DEFAULT '{}'::jsonb,
  function_counts       JSONB       NOT NULL DEFAULT '{}'::jsonb,
  cardinality_summary   JSONB       NOT NULL DEFAULT '{}'::jsonb,
  payload_mode          TEXT        NOT NULL DEFAULT 'full'
    CHECK (payload_mode IN ('full', 'redacted', 'external_ref')),
  program_json          JSONB       NOT NULL DEFAULT '{}'::jsonb,
  program_ref           TEXT,
  created_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ir_programs_kind_created_idx
  ON okg.ir_programs (program_kind, created_at DESC);

CREATE INDEX IF NOT EXISTS ir_programs_registry_idx
  ON okg.ir_programs (registry_id, registry_version, registry_checksum);

CREATE TABLE IF NOT EXISTS okg.ir_program_bindings (
  binding_id       UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  program_id       UUID        NOT NULL
    REFERENCES okg.ir_programs(program_id) ON DELETE CASCADE,
  deployment_name  TEXT        NOT NULL,
  owner_kind       TEXT        NOT NULL
    CHECK (owner_kind IN (
      'graph_api_call', 'mcp_call', 'akmon_call', 'source_run',
      'projection_batch', 'enricher_run', 'gate_eval', 'lens',
      'optimizer_run', 'optimizer_candidate', 'optimizer_evaluation',
      'optimizer_promotion'
    )),
  owner_id         TEXT        NOT NULL,
  program_role     TEXT        NOT NULL
    CHECK (program_role IN (
      'workload', 'match', 'before', 'install', 'after', 'rollback',
      'compiler_input', 'replay', 'source', 'projection', 'enricher',
      'gate', 'lens', 'read', 'promotion'
    )),
  generation_id    TEXT COLLATE "C",
  branch_name      TEXT,
  metadata         JSONB       NOT NULL DEFAULT '{}'::jsonb,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The dedupe index below collapses a NULL generation to '' (D2 retired the
-- numeric -1 sentinel along with the BIGINT column). That is only sound while
-- no real generation ref can BE the empty string, so enforce it rather than
-- assert it in a comment: without this CHECK an empty-string ref written by a
-- caller bug would dedupe against every unbound binding instead of failing.
-- Mirrors admission_envelopes_target_generation_ref_nonempty_chk.
--
-- The ::text carries the same load as in the dedupe index below: schema.sql
-- runs BEFORE the ordered migrations, so on a database that has not yet
-- applied 0023 this statement is parsed while the column is still BIGINT, and
-- btrim(bigint) does not exist. Migration 0023 drops and re-adds this same
-- constraint after the retype, so both paths end with the identical stored
-- definition.
-- Guarded so a converged deployment does not re-take ACCESS EXCLUSIVE and
-- re-validate the whole table on every migrate (migrate.py design edit 4).
DO $ir_binding_generation_nonempty$
BEGIN
  IF NOT EXISTS (
    SELECT 1
      FROM pg_constraint
     WHERE conrelid = 'okg.ir_program_bindings'::regclass
       AND conname = 'ir_program_bindings_generation_id_nonempty_chk'
  ) THEN
    ALTER TABLE okg.ir_program_bindings
      ADD CONSTRAINT ir_program_bindings_generation_id_nonempty_chk
      CHECK (generation_id IS NULL OR length(btrim(generation_id::text)) > 0);
  END IF;
END
$ir_binding_generation_nonempty$;

-- Merge note (D2 vs the acknowledge-runtime-dlq compat repair): `dev` carried a
-- DO block here that converged a legacy TEXT `generation_id` back to BIGINT,
-- because BIGINT was then the canonical type and the dedupe index used the
-- numeric -1 sentinel. D2 reverses that direction: TEXT is canonical, the
-- sentinel is '', and migration 0023 converges the legacy BIGINT column. That
-- block is therefore deleted, not merged. Keeping it would be actively
-- destructive: it runs on every schema replay, so after 0023 it would try to
-- flip the column back on every migrate. Measured, not assumed --- restoring
-- the block and rerunning the guard below aborts migrate with
-- `function btrim(bigint) does not exist`, raised from the D2 nonempty CHECK
-- above, which is no longer well-typed once the column is numeric. On a
-- database holding a real Chronos ref ('okg:...') it instead raises
-- `datatype_mismatch`. Both legacy shapes still converge: a BIGINT column is
-- retyped by 0023, and a pre-collation TEXT column picks up COLLATE "C" there
-- too. `test_schema_replay_keeps_the_ir_binding_generation_ref_text` in
-- tests/substrate/test_migrate.py is the guard.
--
-- Re-confirmed against PR #1081, which hardened that block again while this was
-- in flight and carried its own merge note reaching the same conclusion: the
-- two directions cannot coexist, and this change's deletion is the resolution.
-- #1081 also arrived independently at the mechanism recorded above --- Postgres
-- elides the no-op cast at parse-analysis, so what is STORED is
-- `btrim(generation_id)`, which is exactly what makes a retype to BIGINT abort.
-- Its fix for the bigint direction is to DROP this constraint before
-- converting; correct there, and moot here, because after 0023 nothing converts
-- this column to BIGINT at all.

CREATE INDEX IF NOT EXISTS ir_program_bindings_program_idx
  ON okg.ir_program_bindings (program_id, created_at DESC);

CREATE INDEX IF NOT EXISTS ir_program_bindings_owner_idx
  ON okg.ir_program_bindings (
    deployment_name, owner_kind, owner_id, program_role
  );

CREATE UNIQUE INDEX IF NOT EXISTS ir_program_bindings_dedupe_idx
  ON okg.ir_program_bindings (
    program_id,
    deployment_name,
    owner_kind,
    owner_id,
    program_role,
    -- D2: generation_id is the TEXT checkpoint ref. The dedupe sentinel is the
    -- empty string, matching the sibling COALESCE(branch_name, '') below; the
    -- former numeric -1 sentinel has no TEXT counterpart. '' cannot collide
    -- with a bound generation because
    -- ir_program_bindings_generation_id_nonempty_chk (above) forbids it.
    --
    -- The explicit ::text is load-bearing on the UPGRADE path, not decoration.
    -- migrate() runs schema.sql BEFORE the ordered migrations, so on a database
    -- that has not yet applied 0023 this statement is parsed while the column is
    -- still BIGINT. Postgres parse-analyzes a CREATE INDEX expression BEFORE it
    -- honours IF NOT EXISTS, so a bare COALESCE(generation_id, '') aborts the
    -- whole migrate with "invalid input syntax for type bigint". The cast parses
    -- under both types; on the converged (TEXT) column it is a no-op, so fresh
    -- and migrated store the identical index definition. 0023 re-creates this
    -- index with the same expression.
    COALESCE(generation_id::text, ''),
    COALESCE(branch_name, '')
  );

CREATE TABLE IF NOT EXISTS okg.ir_invocations (
  invocation_id      UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  program_id         UUID        NOT NULL
    REFERENCES okg.ir_programs(program_id) ON DELETE CASCADE,
  binding_id         UUID
    REFERENCES okg.ir_program_bindings(binding_id) ON DELETE SET NULL,
  deployment_name    TEXT        NOT NULL,
  invocation_kind    TEXT        NOT NULL
    CHECK (invocation_kind IN (
      'read', 'score', 'compile', 'materialize', 'replay',
      'promote', 'rollback', 'gate', 'project', 'enrich', 'commit'
    )),
  branch_name        TEXT,
  generation_id      TEXT COLLATE "C",
  status             TEXT        NOT NULL
    CHECK (status IN (
      'planned', 'running', 'succeeded', 'failed', 'rejected', 'skipped'
    )),
  verdict            TEXT,
  reason             TEXT,
  started_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  completed_at       TIMESTAMPTZ,
  result_count       BIGINT      NOT NULL DEFAULT 0,
  node_delta_count   BIGINT      NOT NULL DEFAULT 0,
  edge_delta_count   BIGINT      NOT NULL DEFAULT 0,
  cost_proxy         DOUBLE PRECISION,
  quality_score      DOUBLE PRECISION,
  error_count        BIGINT      NOT NULL DEFAULT 0,
  truncation_count   BIGINT      NOT NULL DEFAULT 0,
  metrics            JSONB       NOT NULL DEFAULT '{}'::jsonb,
  created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ir_invocations_program_idx
  ON okg.ir_invocations (program_id, created_at DESC);

CREATE INDEX IF NOT EXISTS ir_invocations_deployment_kind_idx
  ON okg.ir_invocations (
    deployment_name, invocation_kind, status, created_at DESC
  );

CREATE INDEX IF NOT EXISTS ir_invocations_pin_idx
  ON okg.ir_invocations (
    deployment_name, branch_name, generation_id, invocation_kind
  );

-- Runtime-role access to the IR ledger (PACT route-all-writers / seal-graph-writes):
-- the runtime writer (app_rw) persists its producing transform programs and records
-- invocations as part of the stamp-and-gate write path, and the commit-witness
-- enforcement trigger reads okg.ir_programs to resolve witnesses. Without these the
-- seal is incoherent for app_rw: writers could not ledger their witness, and the
-- trigger could not resolve it. (No DELETE/UPDATE: the ledger is append-only for the
-- runtime role.)
GRANT SELECT, INSERT ON
  okg.ir_programs, okg.ir_program_bindings, okg.ir_invocations
TO app_rw;


-- Commit-witness provenance (PACT route-mutation-through-transform-ir): every
-- durable live row written through the transform IR commit boundary carries the
-- producing program's checksum, which resolves in okg.ir_programs. Additive and
-- nullable so existing rows/deployments are unaffected.

-- Commit-witness provenance on the fact log (PACT route-all-writers, task 2): the
-- source runner + enrichers + admission stamp the producing transform program's
-- checksum on the facts they emit. Additive + nullable.
--
-- The column DEFAULT reads the per-transaction GUC okg.source_program_checksum so a
-- writer that sets it once at its write boundary (e.g. the publisher's enrichment
-- stage, the admission path) auto-stamps EVERY INSERT/COPY whose column list omits
-- the value -- covering the dozen-plus inline enricher/linker INSERT sites without
-- per-site surgery. Writers that pass the column explicitly (the source runner COPY)
-- override the default. missing_ok=true => NULL when the GUC is unset (non-breaking).
--
-- node_facts/edge_facts are columnstore hypertables: TimescaleDB rejects ADD COLUMN
-- with a non-constant default (it would have to rewrite compressed chunks). So add
-- the column plain, then SET DEFAULT separately -- SET DEFAULT only affects future
-- inserts and is accepted on a compressed hypertable.
ALTER TABLE okg.node_facts ADD COLUMN IF NOT EXISTS source_program_checksum TEXT;
ALTER TABLE okg.edge_facts ADD COLUMN IF NOT EXISTS source_program_checksum TEXT;
ALTER TABLE okg.node_facts ALTER COLUMN source_program_checksum
  SET DEFAULT current_setting('okg.source_program_checksum', true);
ALTER TABLE okg.edge_facts ALTER COLUMN source_program_checksum
  SET DEFAULT current_setting('okg.source_program_checksum', true);

-- ===========================================================================
-- Commit-witness seal (PACT seal-graph-writes-behind-the-ir).
--
-- Makes the commit-witness invariant a Postgres-ENFORCED integrity property for
-- the runtime role: any INSERT into a durable graph table whose
-- source_program_checksum does not resolve in okg.ir_programs is rejected. This
-- seals the stamp-and-gate writers route-all-writers shipped -- they stamp a
-- resolvable witness, so they pass; an un-witnessed write is impossible.
--
-- Why a trigger, not a privilege REVOKE: the production writers (projector, source
-- runner, enrichers, admission) issue DIRECT DML + stamp the witness. A REVOKE of
-- INSERT would break them. The trigger enforces witness-resolution without revoking
-- the privilege, and binds even direct DML.
--
-- BOUND (per the PACT): "impossible" means impossible for the runtime role. The
-- trusted base -- a DB superuser or the table owner -- bypasses the trigger by
-- design (it owns the catalog + role discipline already). Enforcement is therefore
-- scoped to non-superuser, non-owner roles (i.e. app_rw, the runtime writer).
--
-- Statement-level with a transition table: ONE antijoin per INSERT statement
-- (incl. chunk-routed COPY into the fact hypertables), not a per-row probe.
CREATE OR REPLACE FUNCTION okg.enforce_commit_witness() RETURNS trigger
LANGUAGE plpgsql AS $fn$
DECLARE
  offending text;
BEGIN
  -- Trusted-base bypass: superuser or the table's owner writes directly.
  IF (SELECT rolsuper FROM pg_roles WHERE rolname = current_user)
     OR current_user = (SELECT tableowner FROM pg_tables
                          WHERE schemaname = TG_TABLE_SCHEMA
                            AND tablename = TG_TABLE_NAME)
  THEN
    RETURN NULL;
  END IF;
  -- Runtime role: every inserted row must carry a witness that resolves in the
  -- IR ledger. NULL checksum does not match -> flagged as un-witnessed.
  SELECT n.source_program_checksum INTO offending
    FROM newrows n
    LEFT JOIN okg.ir_programs p
      ON p.program_checksum = n.source_program_checksum
   WHERE p.program_id IS NULL
   LIMIT 1;
  IF FOUND THEN
    RAISE EXCEPTION
      'commit_witness_missing: durable write into okg.% with unresolvable source_program_checksum %',
      TG_TABLE_NAME, coalesce(offending, '<null>');
  END IF;
  RETURN NULL;
END;
$fn$;

CREATE OR REPLACE TRIGGER enforce_commit_witness
  AFTER INSERT ON okg.node_facts
  REFERENCING NEW TABLE AS newrows
  FOR EACH STATEMENT EXECUTE FUNCTION okg.enforce_commit_witness();
CREATE OR REPLACE TRIGGER enforce_commit_witness
  AFTER INSERT ON okg.edge_facts
  REFERENCING NEW TABLE AS newrows
  FOR EACH STATEMENT EXECUTE FUNCTION okg.enforce_commit_witness();

-- ===========================================================================
-- Read seal (PACT seal-graph-reads-behind-the-ir, app_ro half).
--
-- The agent read role app_ro reads ONLY the registered-surface okg.v_* views,
-- never the base graph tables -- so every read it can make is a read of a
-- registered surface (the IR's read vocabulary). app_ro is already default-denied
-- on all okg tables in views.sql; these explicit REVOKEs name the base GRAPH tables
-- so the read-seal intent is legible alongside the write seal, and re-running is a
-- guard against an accidental future GRANT. The bounded SQL escape hatch (run_sql)
-- independently refuses any relation that is not okg.v_* (mcp/sql_safety.py), so the
-- one raw-read path also cannot reach base tables.
--
-- DEFERRED (per migrate-reads scope): app_rw still holds base-table SELECT because
-- most AKMON operators (search/traverse/inspect/filter/map/aggregate) still read
-- base tables; revoking from app_rw is a follow-on gated on routing those operators
-- (the SEARCH-op lowering + the rest) and granting app_rw SELECT on okg.v_*.
REVOKE SELECT ON
  okg.node_facts, okg.edge_facts
FROM app_ro;
-- Persisted dry-run/apply authority for policy-driven Chronos history compaction.
-- Affected checkpoints enter compacted_generations before the first destructive
-- interval batch so every interleaving read is complete or fails stably.
CREATE TABLE IF NOT EXISTS okg.history_compaction_plans (
  plan_id UUID PRIMARY KEY,
  deployment_name TEXT NOT NULL,
  chronos_branch_id TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('planned', 'applying', 'complete', 'failed')),
  plan JSONB NOT NULL,
  progress JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS okg.compacted_generations (
  checkpoint_id TEXT PRIMARY KEY,
  target_generation_id BIGINT,
  deployment_name TEXT NOT NULL,
  branch_id TEXT NOT NULL,
  plan_id UUID NOT NULL REFERENCES okg.history_compaction_plans(plan_id),
  compacted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  reason TEXT NOT NULL DEFAULT 'history_retention'
);

ALTER TABLE okg.compacted_generations
  ADD COLUMN IF NOT EXISTS target_generation_id BIGINT;

-- NOTE (schema-identity-convergence): this table keeps the bare `branch_id`
-- name, and the reason is mechanical. Applied ordinal
-- 0006_chronos_history_compaction:31-32 re-creates
-- history_generation_pins_checkpoint_idx naming `branch_id`, and every ordered
-- migration replays on a fresh install AFTER schema.sql. Measured on PG 17.10:
-- CREATE INDEX IF NOT EXISTS resolves its column list BEFORE it checks whether
-- the index name already exists, so 0006 aborts a fresh install the moment
-- this column carries a different name -- before migration 0026 is reached to
-- repair it, and an applied ordinal can never be edited. Its sibling
-- okg.history_compaction_plans DID converge because no applied ordinal names
-- its branch column in an index or a LANGUAGE sql body.
CREATE TABLE IF NOT EXISTS okg.history_generation_pins (
  deployment_name TEXT NOT NULL,
  branch_id TEXT NOT NULL,
  pin_name TEXT NOT NULL,
  checkpoint_id TEXT NOT NULL,
  expires_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (deployment_name, branch_id, pin_name)
);

CREATE INDEX IF NOT EXISTS history_generation_pins_checkpoint_idx
  ON okg.history_generation_pins (deployment_name, branch_id, checkpoint_id);

CREATE UNIQUE INDEX IF NOT EXISTS compacted_generations_target_id_idx
  ON okg.compacted_generations (deployment_name, target_generation_id)
  WHERE target_generation_id IS NOT NULL;

CREATE OR REPLACE FUNCTION okg.lock_history_compaction_protection()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  PERFORM pg_advisory_xact_lock(1326131011, 1);
  RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
END;
$$;

DROP TRIGGER IF EXISTS history_generation_pins_coordination
  ON okg.history_generation_pins;
CREATE TRIGGER history_generation_pins_coordination
BEFORE INSERT OR UPDATE OR DELETE ON okg.history_generation_pins
FOR EACH STATEMENT EXECUTE FUNCTION okg.lock_history_compaction_protection();

DO $$
BEGIN
  IF to_regclass('public._chronos_branch_interval_branches') IS NOT NULL THEN
    EXECUTE 'DROP TRIGGER IF EXISTS chronos_branch_history_coordination '
            'ON public._chronos_branch_interval_branches';
    EXECUTE 'CREATE TRIGGER chronos_branch_history_coordination '
            'BEFORE INSERT OR UPDATE OR DELETE '
            'ON public._chronos_branch_interval_branches '
            'FOR EACH STATEMENT EXECUTE FUNCTION okg.lock_history_compaction_protection()';
  END IF;
  IF to_regclass('public._chronos_branch_interval_checkpoints') IS NOT NULL THEN
    EXECUTE 'DROP TRIGGER IF EXISTS chronos_checkpoint_history_coordination '
            'ON public._chronos_branch_interval_checkpoints';
    EXECUTE 'CREATE TRIGGER chronos_checkpoint_history_coordination '
            'BEFORE INSERT OR UPDATE OR DELETE '
            'ON public._chronos_branch_interval_checkpoints '
            'FOR EACH STATEMENT EXECUTE FUNCTION okg.lock_history_compaction_protection()';
  END IF;
END;
$$;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_ro') THEN
    GRANT SELECT ON okg.compacted_generations TO app_ro;
  END IF;
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_rw') THEN
    GRANT SELECT ON okg.compacted_generations TO app_rw;
    GRANT SELECT, INSERT, UPDATE, DELETE
      ON okg.history_compaction_plans, okg.history_generation_pins TO app_rw;
  END IF;
END;
$$;

CREATE OR REPLACE FUNCTION okg.acquire_history_generation_read_lease(
  p_deployment_name TEXT,
  p_branch_id TEXT,
  p_pin_name TEXT,
  p_checkpoint_id TEXT
) RETURNS BOOLEAN
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, okg, public
AS $$
BEGIN
  IF p_pin_name NOT LIKE 'read:%' THEN
    RAISE EXCEPTION 'read lease name must start with read:';
  END IF;
  PERFORM pg_advisory_xact_lock(1326131011, 1);
  DELETE FROM okg.history_generation_pins
   WHERE ctid IN (
     SELECT ctid FROM okg.history_generation_pins
      WHERE pin_name LIKE 'read:%' AND expires_at <= now()
      LIMIT 1000
   );
  IF EXISTS (
       SELECT 1 FROM okg.compacted_generations
        WHERE checkpoint_id = p_checkpoint_id
     )
     OR NOT EXISTS (
       SELECT 1 FROM public._chronos_branch_interval_checkpoints
        WHERE checkpoint_id = p_checkpoint_id
          AND branch_id = p_branch_id
          AND metadata::jsonb ->> 'kind' = 'okg_generation'
          AND metadata::jsonb ->> 'deployment_name' = p_deployment_name
     )
  THEN
    RETURN FALSE;
  END IF;
  INSERT INTO okg.history_generation_pins
    (deployment_name, branch_id, pin_name, checkpoint_id, expires_at)
  VALUES
    (p_deployment_name, p_branch_id, p_pin_name, p_checkpoint_id,
     now() + interval '5 minutes')
  ON CONFLICT (deployment_name, branch_id, pin_name) DO UPDATE
    SET checkpoint_id = EXCLUDED.checkpoint_id,
        expires_at = EXCLUDED.expires_at;
  RETURN TRUE;
END;
$$;

CREATE OR REPLACE FUNCTION okg.release_history_generation_read_lease(
  p_deployment_name TEXT,
  p_branch_id TEXT,
  p_pin_name TEXT
) RETURNS BOOLEAN
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, okg, public
AS $$
DECLARE
  removed INTEGER;
BEGIN
  IF p_pin_name NOT LIKE 'read:%' THEN
    RETURN FALSE;
  END IF;
  PERFORM pg_advisory_xact_lock(1326131011, 1);
  DELETE FROM okg.history_generation_pins
   WHERE deployment_name = p_deployment_name
     AND branch_id = p_branch_id
     AND pin_name = p_pin_name;
  GET DIAGNOSTICS removed = ROW_COUNT;
  RETURN removed > 0;
END;
$$;

REVOKE ALL ON FUNCTION okg.acquire_history_generation_read_lease(TEXT, TEXT, TEXT, TEXT)
  FROM PUBLIC;
REVOKE ALL ON FUNCTION okg.release_history_generation_read_lease(TEXT, TEXT, TEXT)
  FROM PUBLIC;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_ro') THEN
    GRANT EXECUTE
      ON FUNCTION okg.acquire_history_generation_read_lease(TEXT, TEXT, TEXT, TEXT)
      TO app_ro;
    GRANT EXECUTE
      ON FUNCTION okg.release_history_generation_read_lease(TEXT, TEXT, TEXT)
      TO app_ro;
  END IF;
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_rw') THEN
    GRANT EXECUTE
      ON FUNCTION okg.acquire_history_generation_read_lease(TEXT, TEXT, TEXT, TEXT)
      TO app_rw;
    GRANT EXECUTE
      ON FUNCTION okg.release_history_generation_read_lease(TEXT, TEXT, TEXT)
      TO app_rw;
  END IF;
END;
$$;

CREATE OR REPLACE FUNCTION okg.compacted_generation_record(
  p_generation TEXT,
  p_deployment_name TEXT DEFAULT NULL
) RETURNS JSONB
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $$
  SELECT jsonb_build_object(
    'checkpoint_id', checkpoint_id,
    'target_generation_id', target_generation_id,
    'deployment_name', deployment_name,
    'branch', branch_id,
    'plan_id', plan_id::text,
    'compacted_at', compacted_at,
    'reason', reason
  )
  FROM okg.compacted_generations
  WHERE (
    checkpoint_id = p_generation
    OR (
      p_deployment_name IS NOT NULL
      AND target_generation_id = CASE
        WHEN p_generation ~ '^[0-9]+$' THEN p_generation::bigint
        ELSE NULL
      END
    )
  )
  AND (p_deployment_name IS NULL OR deployment_name = p_deployment_name)
  ORDER BY compacted_at DESC
  LIMIT 1
$$;

REVOKE ALL ON FUNCTION okg.compacted_generation_record(TEXT, TEXT) FROM PUBLIC;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_ro') THEN
    GRANT EXECUTE ON FUNCTION okg.compacted_generation_record(TEXT, TEXT) TO app_ro;
  END IF;
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_rw') THEN
    GRANT EXECUTE ON FUNCTION okg.compacted_generation_record(TEXT, TEXT) TO app_rw;
  END IF;
END;
$$;

-- ========== PACT ledger — okg.pact_decisions / _intents / _events ==========
--
-- Namespace contract (docs/reference/database-schema.md §4.1, D4):
-- everything application-owned lives in `okg`. These three tables were the
-- last application relations created unqualified (they resolved to `public`
-- via search_path from `okg/substrate/pact/decisions.py`), and they were
-- runtime-only — created on first ledger touch, absent from this snapshot
-- (2026-07-28 audit, F2 family). Both defects close here: the snapshot owns
-- the shape, in `okg`.
--
-- The guard below is load-bearing, not defensive noise. Migration
-- 0022_pact_ledger_schema_qualification moves a pre-qualification `public`
-- ledger into `okg` with ALTER TABLE ... SET SCHEMA, and the ordered runner
-- executes AFTER this body (migrate.py: schema body -> ordered migrations ->
-- views). An unguarded CREATE here would mint an empty `okg` shell first and
-- the move would fail on the name collision, stranding the operator's rows in
-- `public`. So: create only when no `public` predecessor is waiting to be
-- adopted. On every fresh install `to_regclass` is NULL and all three are
-- created here.
--
-- okg_mcp's INSERT+SELECT on pact_coordination_events is deliberately NOT
-- granted here: views.sql's `REVOKE ALL ON ALL TABLES IN SCHEMA okg FROM
-- okg_mcp` runs after this file and would strip it. The grant is reconciled
-- by the ledger probe-guard in `okg/substrate/pact/decisions.py`, and moves
-- to the grant registry when the schema-grant-coherence child lands.
DO $pact_ledger_tables$
BEGIN
  IF to_regclass('public.pact_decisions') IS NULL THEN
    CREATE TABLE IF NOT EXISTS okg.pact_decisions (
      id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      change_id     TEXT NOT NULL,
      verdict       TEXT NOT NULL,
      note          TEXT,
      target        TEXT,
      principal     TEXT NOT NULL,
      authority     TEXT NOT NULL DEFAULT 'operator',
      intent_digest TEXT,
      from_state    TEXT,
      to_state      TEXT,
      decided_at    TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    CREATE INDEX IF NOT EXISTS pact_decisions_change_idx
      ON okg.pact_decisions (change_id, id);
  END IF;

  IF to_regclass('public.pact_intents') IS NULL THEN
    CREATE TABLE IF NOT EXISTS okg.pact_intents (
      change_id       TEXT NOT NULL,
      worktree_suffix TEXT NOT NULL,
      worktree_path   TEXT,
      branch          TEXT,
      head_sha        TEXT,
      session_id      TEXT,
      intent_digest   TEXT,
      derived_state   TEXT,
      registered_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
      last_event_at   TIMESTAMPTZ,
      PRIMARY KEY (change_id, worktree_suffix)
    );
  END IF;

  IF to_regclass('public.pact_coordination_events') IS NULL THEN
    CREATE TABLE IF NOT EXISTS okg.pact_coordination_events (
      id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      event_type      TEXT NOT NULL,
      change_id       TEXT NOT NULL,
      task_id         TEXT,
      worktree_suffix TEXT,
      worktree_path   TEXT,
      branch          TEXT,
      head_sha        TEXT,
      session_id      TEXT,
      principal       TEXT,
      intent_digest   TEXT,
      payload         JSONB NOT NULL DEFAULT '{}'::jsonb,
      occurred_at     TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    CREATE INDEX IF NOT EXISTS pact_coordination_events_change_idx
      ON okg.pact_coordination_events (change_id, id);
    CREATE INDEX IF NOT EXISTS pact_coordination_events_worktree_idx
      ON okg.pact_coordination_events (worktree_suffix, occurred_at DESC);
  END IF;
END
$pact_ledger_tables$;

-- Late-sequence grant catch-up. The bulk `GRANT USAGE, SELECT ON ALL
-- SEQUENCES` in the role-grant section above is point-in-time: tables
-- created AFTER it in this file (okg.llm_calls, okg.envelope_violations)
-- mint their sequences too late to be covered, so a single-pass fresh
-- install under-granted relative to a re-migrated database (whose
-- replay re-runs the bulk grant with every sequence already present).
-- The migration-convergence lane caught the divergence; this final
-- pass closes it identically on both sides.
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA okg TO app_rw;
-- External action review ledger.  This migration is intentionally additive and
-- idempotent: schema.sql contains the same current-state definitions for fresh
-- installs, while this file upgrades deployments that have already applied the
-- preceding migration ordinals.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

-- Existing installs used the generic ``deployment`` identity column. Rename
-- it before replaying any canonical table DDL: CREATE TABLE IF NOT EXISTS does
-- not repair an existing table, and later foreign keys already name the
-- canonical column. A mixed state is refused so the migration never leaves
-- two competing identity columns behind.
DO $external_action_deployment_identity_cutover$
DECLARE
  relation_name TEXT;
  has_legacy_column BOOLEAN;
  has_canonical_column BOOLEAN;
BEGIN
  FOREACH relation_name IN ARRAY ARRAY[
    'external_action_proposals',
    'external_action_payload_versions',
    'external_action_auth_audits',
    'external_action_dpop_replay_nonces',
    'external_action_decisions',
    'external_action_transition_idempotency',
    'external_action_executions',
    'external_action_owner_unavailability_attestations',
    'external_action_outcomes',
    'external_action_events',
    'external_action_execution_epoch_leases',
    'external_action_restore_attestations',
    'external_action_restore_fences',
    'external_action_restore_sessions',
    'external_action_restore_completion_receipts',
    'external_action_restore_finalize_idempotency',
    'external_action_restore_checkpoint_staging',
    'external_action_restore_checkpoint_entries',
    'external_action_restore_recoveries',
    'external_action_api_role_bindings'
  ] LOOP
    SELECT EXISTS (
      SELECT 1
        FROM pg_catalog.pg_attribute a
        JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
       WHERE n.nspname = 'okg'
         AND c.relname = relation_name
         AND c.relkind = 'r'
         AND a.attname = 'deployment'
         AND NOT a.attisdropped
    ), EXISTS (
      SELECT 1
        FROM pg_catalog.pg_attribute a
        JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
       WHERE n.nspname = 'okg'
         AND c.relname = relation_name
         AND c.relkind = 'r'
         AND a.attname = 'deployment_name'
         AND NOT a.attisdropped
    )
      INTO has_legacy_column, has_canonical_column;
    IF has_legacy_column AND has_canonical_column THEN
      RAISE EXCEPTION 'external action relation % has both deployment identity columns', relation_name
        USING ERRCODE = '42701';
    ELSIF has_legacy_column THEN
      EXECUTE format(
        'ALTER TABLE okg.%I RENAME COLUMN %I TO %I',
        relation_name, 'deployment', 'deployment_name'
      );
    END IF;
  END LOOP;
END
$external_action_deployment_identity_cutover$;

CREATE TABLE IF NOT EXISTS okg.external_action_proposals (
  proposal_id TEXT PRIMARY KEY,
  deployment_name TEXT NOT NULL CHECK (btrim(deployment_name) <> ''),
  execution_epoch BIGINT NOT NULL CHECK (execution_epoch >= 0),
  effect_instance_key TEXT NOT NULL CHECK (btrim(effect_instance_key) <> ''),
  submission_content_hash TEXT NOT NULL,
  submission_request_hash TEXT NOT NULL,
  submission_idempotency_key_hash TEXT NOT NULL,
  proposer_separation_subject TEXT NOT NULL,
  automation_id TEXT NOT NULL, automation_version TEXT NOT NULL,
  run_id TEXT NOT NULL, correlation_id TEXT NOT NULL,
  action_type TEXT NOT NULL, target_ref TEXT NOT NULL, risk_tier TEXT NOT NULL,
  action_schema_digest TEXT NOT NULL, action_contract_digest TEXT NOT NULL,
  executor_contract_digest TEXT NOT NULL, payload_renderer_digest TEXT NOT NULL,
  governed_tool_binding_digest TEXT NOT NULL,
  provider_connection_contract_digest TEXT NOT NULL,
  provider_capability_digest TEXT NOT NULL, provider_operation_id TEXT NOT NULL,
  provider_operation_descriptor_digest TEXT NOT NULL,
  original_payload_hash TEXT NOT NULL,
  requested_review_expires_at TIMESTAMPTZ NOT NULL,
  effective_review_expires_at TIMESTAMPTZ NOT NULL,
  summary_code TEXT NOT NULL,
  evidence_digests JSONB NOT NULL DEFAULT '[]'::jsonb,
  review_state TEXT NOT NULL DEFAULT 'pending'
    CHECK (review_state IN ('pending','edited','approved','rejected','expired','cancelled')),
  execution_state TEXT NOT NULL DEFAULT 'unavailable'
    CHECK (execution_state IN ('unavailable','ready','claimed','succeeded','failed','indeterminate')),
  lifecycle_version BIGINT NOT NULL DEFAULT 1 CHECK (lifecycle_version > 0),
  payload_version BIGINT NOT NULL DEFAULT 1 CHECK (payload_version > 0),
  current_payload_hash TEXT NOT NULL,
  current_decision_id TEXT, current_decision_binding_hash TEXT,
  execute_before TIMESTAMPTZ, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), expired_at TIMESTAMPTZ,
  UNIQUE (deployment_name, proposal_id), UNIQUE (deployment_name, effect_instance_key),
  UNIQUE (deployment_name, proposer_separation_subject, submission_idempotency_key_hash)
);

CREATE TABLE IF NOT EXISTS okg.external_action_payload_versions (
  payload_version_id BIGSERIAL PRIMARY KEY, deployment_name TEXT NOT NULL,
  proposal_id TEXT NOT NULL, payload_version BIGINT NOT NULL,
  payload_hash TEXT NOT NULL, parent_payload_hash TEXT,
  canonical_payload BYTEA NOT NULL,
  difference_field_codes JSONB NOT NULL DEFAULT '[]'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (deployment_name, proposal_id, payload_version),
  UNIQUE (deployment_name, proposal_id, payload_hash),
  FOREIGN KEY (deployment_name, proposal_id)
    REFERENCES okg.external_action_proposals(deployment_name, proposal_id)
    ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS okg.external_action_auth_audits (
  authentication_audit_id TEXT PRIMARY KEY,
  deployment_name TEXT NOT NULL CHECK (btrim(deployment_name) <> ''),
  request_id TEXT NOT NULL, principal TEXT NOT NULL,
  separation_subject TEXT NOT NULL, nomos_provider_id TEXT NOT NULL,
  nomos_subject_binding_id TEXT NOT NULL, authentication_method_code TEXT NOT NULL,
  issuer_or_trust_domain_digest TEXT NOT NULL,
  sender_key_or_certificate_thumbprint TEXT, token_or_certificate_id_digest TEXT,
  dpop_jti_digest TEXT, auth_config_digest TEXT NOT NULL,
  result_code TEXT NOT NULL, observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  credential_expires_at TIMESTAMPTZ,
  provider_status TEXT, binding_status TEXT,
  provider_updated_at TIMESTAMPTZ, binding_updated_at TIMESTAMPTZ,
  provider_credential_generation_digest TEXT,
  binding_credential_generation_digest TEXT,
  credential_generation_digest TEXT,
  presented_credential_generation_digest TEXT,
  token_issued_at TIMESTAMPTZ,
  certificate_thumbprint TEXT,
  certificate_not_before TIMESTAMPTZ,
  certificate_not_after TIMESTAMPTZ,
  verifier_assertion_nonce_digest TEXT,
  verifier_assertion_expires_at TIMESTAMPTZ,
  verifier_transition_digest TEXT,
  verifier_policy_input_digest TEXT,
  verifier_required_obligations_digest TEXT,
  verifier_completed_obligations_digest TEXT,
  verifier_missing_obligations_digest TEXT,
  verifier_assertion_tag TEXT,
  verifier_capability_generation BIGINT,
  CONSTRAINT external_action_auth_audits_provider_status_check
    CHECK (provider_status IS NULL OR provider_status IN ('active','disabled','rotating','revoked')),
  CONSTRAINT external_action_auth_audits_binding_status_check
    CHECK (binding_status IS NULL OR binding_status IN ('active','disabled')),
  CONSTRAINT external_action_auth_audits_provider_generation_check
    CHECK (provider_credential_generation_digest IS NULL OR provider_credential_generation_digest ~ '^sha256:[0-9a-f]{64}$'),
  CONSTRAINT external_action_auth_audits_binding_generation_check
    CHECK (binding_credential_generation_digest IS NULL OR binding_credential_generation_digest ~ '^sha256:[0-9a-f]{64}$'),
  CONSTRAINT external_action_auth_audits_credential_generation_check
    CHECK (credential_generation_digest IS NULL OR credential_generation_digest ~ '^sha256:[0-9a-f]{64}$'),
  CONSTRAINT external_action_auth_audits_presented_generation_check
    CHECK (presented_credential_generation_digest IS NULL OR presented_credential_generation_digest ~ '^sha256:[0-9a-f]{64}$'),
  CONSTRAINT external_action_auth_audits_certificate_thumbprint_check
    CHECK (certificate_thumbprint IS NULL OR (btrim(certificate_thumbprint) = certificate_thumbprint AND octet_length(certificate_thumbprint) BETWEEN 1 AND 512)),
  CONSTRAINT external_action_auth_audits_certificate_interval_check
    CHECK (certificate_not_before IS NULL OR certificate_not_after IS NULL OR certificate_not_after >= certificate_not_before),
  nomos_decision_id TEXT,
  UNIQUE (deployment_name, authentication_audit_id),
  UNIQUE (deployment_name, verifier_capability_generation, verifier_assertion_nonce_digest),
  FOREIGN KEY (deployment_name, nomos_decision_id)
    REFERENCES okg.nomos_decisions(deployment_name, decision_id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS okg.external_action_dpop_replay_nonces (
  dpop_replay_nonce_id BIGSERIAL PRIMARY KEY, deployment_name TEXT NOT NULL,
  issuer_digest TEXT NOT NULL, sender_key_thumbprint TEXT NOT NULL,
  jti_digest TEXT NOT NULL, request_method TEXT NOT NULL,
  normalized_request_uri_digest TEXT NOT NULL, access_token_hash TEXT NOT NULL,
  observed_at TIMESTAMPTZ NOT NULL DEFAULT now(), expires_at TIMESTAMPTZ NOT NULL,
  UNIQUE (deployment_name, issuer_digest, sender_key_thumbprint, jti_digest)
);

CREATE TABLE IF NOT EXISTS okg.external_action_decisions (
  decision_row_id BIGSERIAL PRIMARY KEY, decision_id TEXT NOT NULL,
  deployment_name TEXT NOT NULL, proposal_id TEXT NOT NULL,
  verdict TEXT NOT NULL CHECK (verdict IN ('approved','rejected')),
  expected_lifecycle_version BIGINT NOT NULL, expected_payload_version BIGINT NOT NULL,
  approved_lifecycle_version BIGINT NOT NULL, approved_payload_version BIGINT NOT NULL,
  approved_payload_hash TEXT NOT NULL, action_type TEXT NOT NULL,
  target_ref TEXT NOT NULL, action_schema_digest TEXT NOT NULL,
  action_contract_digest TEXT NOT NULL, executor_contract_digest TEXT NOT NULL,
  payload_renderer_digest TEXT NOT NULL, governed_tool_binding_digest TEXT NOT NULL,
  provider_connection_contract_digest TEXT NOT NULL, provider_capability_digest TEXT NOT NULL,
  provider_operation_id TEXT NOT NULL, provider_operation_descriptor_digest TEXT NOT NULL,
  execution_epoch BIGINT NOT NULL, effect_instance_key TEXT NOT NULL,
  approver_principal TEXT NOT NULL, approver_separation_subject TEXT NOT NULL,
  authentication_audit_id TEXT NOT NULL, nomos_decision_id TEXT NOT NULL,
  policy_digest TEXT NOT NULL, execute_before TIMESTAMPTZ, reason_code TEXT,
  decision_binding_hash TEXT NOT NULL, request_hash TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(), UNIQUE (deployment_name, decision_id),
  FOREIGN KEY (deployment_name, proposal_id)
    REFERENCES okg.external_action_proposals(deployment_name, proposal_id) ON DELETE RESTRICT,
  FOREIGN KEY (deployment_name, authentication_audit_id)
    REFERENCES okg.external_action_auth_audits(deployment_name, authentication_audit_id) ON DELETE RESTRICT,
  FOREIGN KEY (deployment_name, nomos_decision_id)
    REFERENCES okg.nomos_decisions(deployment_name, decision_id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS okg.external_action_transition_idempotency (
  transition_idempotency_id BIGSERIAL PRIMARY KEY, deployment_name TEXT NOT NULL,
  proposal_id TEXT NOT NULL,
  operation TEXT NOT NULL CHECK (operation IN ('edit','defer','approve','approve_with_edit','reject','cancel')),
  actor_separation_subject TEXT NOT NULL, idempotency_key_hash TEXT NOT NULL,
  transition_request_hash TEXT NOT NULL, response_status INTEGER NOT NULL,
  response_media_type TEXT NOT NULL, response_body_jcs BYTEA NOT NULL,
  result_event_ids JSONB NOT NULL DEFAULT '[]'::jsonb, result_payload_version BIGINT,
  result_decision_id TEXT, result_lifecycle_version BIGINT, result_etag TEXT,
  committed_at TIMESTAMPTZ NOT NULL DEFAULT now(), authentication_audit_id TEXT NOT NULL,
  nomos_decision_id TEXT NOT NULL,
  UNIQUE (deployment_name, proposal_id, actor_separation_subject, idempotency_key_hash),
  FOREIGN KEY (deployment_name, proposal_id)
    REFERENCES okg.external_action_proposals(deployment_name, proposal_id) ON DELETE RESTRICT,
  FOREIGN KEY (deployment_name, authentication_audit_id)
    REFERENCES okg.external_action_auth_audits(deployment_name, authentication_audit_id) ON DELETE RESTRICT,
  FOREIGN KEY (deployment_name, nomos_decision_id)
    REFERENCES okg.nomos_decisions(deployment_name, decision_id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS okg.external_action_executions (
  execution_id TEXT PRIMARY KEY, deployment_name TEXT NOT NULL, proposal_id TEXT NOT NULL,
  decision_id TEXT NOT NULL, execution_epoch BIGINT NOT NULL,
  execution_epoch_lease_digest TEXT NOT NULL, effect_instance_key TEXT NOT NULL,
  worker_separation_subject TEXT NOT NULL, approved_lifecycle_version BIGINT NOT NULL,
  approved_payload_version BIGINT NOT NULL, approved_payload_hash TEXT NOT NULL,
  action_type TEXT NOT NULL, target_ref TEXT NOT NULL, action_schema_digest TEXT NOT NULL,
  action_contract_digest TEXT NOT NULL, executor_contract_digest TEXT NOT NULL,
  payload_renderer_digest TEXT NOT NULL, governed_tool_binding_digest TEXT NOT NULL,
  provider_connection_contract_digest TEXT NOT NULL, provider_capability_digest TEXT NOT NULL,
  provider_operation_id TEXT NOT NULL, provider_operation_descriptor_digest TEXT NOT NULL,
  policy_digest TEXT NOT NULL, execution_binding_hash TEXT NOT NULL,
  request_hash TEXT NOT NULL, okg_claim_handoff_receipt_digest TEXT NOT NULL,
  handoff_protected_b64 TEXT NOT NULL, handoff_payload_b64 TEXT NOT NULL,
  handoff_signature_b64 TEXT NOT NULL, canonical_payload BYTEA NOT NULL,
  provider_idempotency_key_digest TEXT NOT NULL, authentication_audit_id TEXT NOT NULL,
  nomos_decision_id TEXT NOT NULL, claimed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  execute_before TIMESTAMPTZ NOT NULL, UNIQUE (deployment_name, proposal_id),
  FOREIGN KEY (deployment_name, proposal_id)
    REFERENCES okg.external_action_proposals(deployment_name, proposal_id) ON DELETE RESTRICT,
  FOREIGN KEY (deployment_name, decision_id)
    REFERENCES okg.external_action_decisions(deployment_name, decision_id) ON DELETE RESTRICT,
  FOREIGN KEY (deployment_name, authentication_audit_id)
    REFERENCES okg.external_action_auth_audits(deployment_name, authentication_audit_id) ON DELETE RESTRICT,
  FOREIGN KEY (deployment_name, nomos_decision_id)
    REFERENCES okg.nomos_decisions(deployment_name, decision_id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS okg.external_action_owner_unavailability_attestations (
  attestation_id BIGSERIAL PRIMARY KEY, deployment_name TEXT NOT NULL, execution_id TEXT NOT NULL,
  claim_owner_separation_subject TEXT NOT NULL, reconcile_not_before TIMESTAMPTZ NOT NULL,
  issued_at TIMESTAMPTZ NOT NULL, expires_at TIMESTAMPTZ NOT NULL, nonce TEXT NOT NULL,
  signer_separation_subject TEXT NOT NULL, authenticated_submitter_separation_subject TEXT NOT NULL,
  idempotency_key_hash TEXT NOT NULL, request_hash TEXT NOT NULL, response_status INTEGER NOT NULL,
  response_body_jcs BYTEA NOT NULL, protected_b64 TEXT NOT NULL, payload_b64 TEXT NOT NULL,
  signature_b64 TEXT NOT NULL, signer_key_id_digest TEXT NOT NULL, envelope_digest TEXT NOT NULL,
  verified_at TIMESTAMPTZ NOT NULL DEFAULT now(), authentication_audit_id TEXT NOT NULL,
  nomos_decision_id TEXT NOT NULL,
  UNIQUE (deployment_name, signer_separation_subject, nonce), UNIQUE (deployment_name, envelope_digest),
  UNIQUE (deployment_name, execution_id, envelope_digest),
  FOREIGN KEY (execution_id) REFERENCES okg.external_action_executions(execution_id) ON DELETE RESTRICT,
  FOREIGN KEY (deployment_name, authentication_audit_id)
    REFERENCES okg.external_action_auth_audits(deployment_name, authentication_audit_id) ON DELETE RESTRICT,
  FOREIGN KEY (deployment_name, nomos_decision_id)
    REFERENCES okg.nomos_decisions(deployment_name, decision_id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS okg.external_action_outcomes (
  outcome_id BIGSERIAL PRIMARY KEY, deployment_name TEXT NOT NULL, execution_id TEXT NOT NULL,
  effect_instance_key TEXT NOT NULL, worker_or_reconciler_separation_subject TEXT NOT NULL,
  outcome TEXT NOT NULL CHECK (outcome IN ('succeeded','failed','indeterminate')),
  outcome_reason_code TEXT NOT NULL, provider_receipt_digest TEXT,
  recovery_journal_digest TEXT NOT NULL, owner_unavailability_attestation_digest TEXT,
  request_hash TEXT NOT NULL, authentication_audit_id TEXT NOT NULL,
  nomos_decision_id TEXT NOT NULL, recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (deployment_name, execution_id),
  FOREIGN KEY (execution_id) REFERENCES okg.external_action_executions(execution_id) ON DELETE RESTRICT,
  FOREIGN KEY (deployment_name, execution_id, owner_unavailability_attestation_digest)
    REFERENCES okg.external_action_owner_unavailability_attestations(deployment_name, execution_id, envelope_digest) ON DELETE RESTRICT,
  FOREIGN KEY (deployment_name, authentication_audit_id)
    REFERENCES okg.external_action_auth_audits(deployment_name, authentication_audit_id) ON DELETE RESTRICT,
  FOREIGN KEY (deployment_name, nomos_decision_id)
    REFERENCES okg.nomos_decisions(deployment_name, decision_id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS okg.external_action_events (
  event_id BIGSERIAL PRIMARY KEY, deployment_name TEXT NOT NULL, proposal_id TEXT,
  execution_epoch BIGINT NOT NULL, effect_instance_key TEXT NOT NULL,
  sequence BIGINT NOT NULL, lifecycle_version BIGINT NOT NULL,
  event_type TEXT NOT NULL, actor_kind TEXT NOT NULL,
  actor_principal TEXT, actor_separation_subject TEXT,
  authentication_audit_ref TEXT, nomos_decision_ref TEXT, deadline_kind TEXT,
  policy_digest TEXT, before_review_state TEXT, after_review_state TEXT,
  before_execution_state TEXT, after_execution_state TEXT, payload_version BIGINT,
  payload_hash TEXT, decision_id TEXT, decision_binding_hash TEXT,
  execution_id TEXT, execution_binding_hash TEXT, transition_reason_code TEXT,
  outcome TEXT, outcome_reason_code TEXT, outcome_request_hash TEXT,
  restore_recovery_digest TEXT, effective_at TIMESTAMPTZ,
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(), previous_event_hash TEXT,
  event_hash TEXT NOT NULL, UNIQUE (deployment_name, effect_instance_key, sequence),
  UNIQUE (deployment_name, effect_instance_key, event_hash),
  FOREIGN KEY (deployment_name, proposal_id)
    REFERENCES okg.external_action_proposals(deployment_name, proposal_id) ON DELETE RESTRICT,
  FOREIGN KEY (deployment_name, authentication_audit_ref)
    REFERENCES okg.external_action_auth_audits(deployment_name, authentication_audit_id) ON DELETE RESTRICT,
  FOREIGN KEY (deployment_name, nomos_decision_ref)
    REFERENCES okg.nomos_decisions(deployment_name, decision_id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS okg.external_action_execution_epoch_leases (
  lease_row_id BIGSERIAL PRIMARY KEY, deployment_name TEXT NOT NULL, epoch BIGINT NOT NULL,
  lease_sequence BIGINT NOT NULL, previous_lease_digest TEXT, previous_epoch_digest TEXT,
  issued_at TIMESTAMPTZ NOT NULL, expires_at TIMESTAMPTZ NOT NULL,
  signer_separation_subject TEXT NOT NULL, nonce TEXT NOT NULL,
  protected_b64 TEXT NOT NULL, payload_b64 TEXT NOT NULL, signature_b64 TEXT NOT NULL,
  signer_key_id_digest TEXT NOT NULL, envelope_digest TEXT NOT NULL,
  verified_at TIMESTAMPTZ NOT NULL DEFAULT now(), verification_source TEXT NOT NULL,
  verification_config_digest TEXT NOT NULL, verification_login_binding_digest TEXT NOT NULL,
  lease_chain_digest TEXT, authentication_audit_id TEXT, nomos_decision_id TEXT,
  UNIQUE (deployment_name, epoch, lease_sequence),
  UNIQUE (deployment_name, signer_separation_subject, nonce), UNIQUE (deployment_name, envelope_digest),
  FOREIGN KEY (deployment_name, authentication_audit_id)
    REFERENCES okg.external_action_auth_audits(deployment_name, authentication_audit_id) ON DELETE RESTRICT,
  FOREIGN KEY (deployment_name, nomos_decision_id)
    REFERENCES okg.nomos_decisions(deployment_name, decision_id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS okg.external_action_restore_attestations (
  attestation_row_id BIGSERIAL PRIMARY KEY, deployment_name TEXT NOT NULL, restore_id TEXT NOT NULL,
  old_epoch BIGINT NOT NULL, new_epoch BIGINT NOT NULL, old_epoch_lease_digest TEXT NOT NULL,
  new_epoch_lease_digest TEXT NOT NULL, ledger_fence_envelope_digest TEXT NOT NULL,
  fenced_ledger_root_digest TEXT NOT NULL, fenced_ledger_entry_count BIGINT NOT NULL,
  restored_snapshot_digest TEXT NOT NULL, pre_restore_event_root_digest TEXT NOT NULL,
  pre_restore_event_head_count BIGINT NOT NULL, effect_inventory_digest TEXT NOT NULL,
  effect_inventory_count BIGINT NOT NULL, worker_checkpoint_set_digest TEXT NOT NULL,
  worker_checkpoint_count BIGINT NOT NULL, issued_at TIMESTAMPTZ NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL, nonce TEXT NOT NULL, signer_separation_subject TEXT NOT NULL,
  protected_b64 TEXT NOT NULL, payload_b64 TEXT NOT NULL, signature_b64 TEXT NOT NULL,
  signer_key_id_digest TEXT NOT NULL, envelope_digest TEXT NOT NULL,
  verified_at TIMESTAMPTZ NOT NULL DEFAULT now(), authentication_audit_id TEXT NOT NULL,
  nomos_decision_id TEXT NOT NULL, UNIQUE (deployment_name, restore_id),
  UNIQUE (deployment_name, signer_separation_subject, nonce), UNIQUE (deployment_name, envelope_digest),
  FOREIGN KEY (deployment_name, authentication_audit_id)
    REFERENCES okg.external_action_auth_audits(deployment_name, authentication_audit_id) ON DELETE RESTRICT,
  FOREIGN KEY (deployment_name, nomos_decision_id)
    REFERENCES okg.nomos_decisions(deployment_name, decision_id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS okg.external_action_restore_fences (
  deployment_name TEXT PRIMARY KEY, highest_accepted_epoch BIGINT NOT NULL, lease_digest TEXT NOT NULL,
  quarantine_state TEXT NOT NULL CHECK (quarantine_state IN ('clear','blocked')),
  service_mode TEXT NOT NULL CHECK (service_mode IN ('restore_only','normal')),
  lease_chain_digest TEXT NOT NULL, verification_config_digest TEXT NOT NULL,
  verification_login_binding_digest TEXT NOT NULL, restore_attestation_digest TEXT,
  accepted_at TIMESTAMPTZ NOT NULL DEFAULT now(), mode_changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  FOREIGN KEY (deployment_name, restore_attestation_digest)
    REFERENCES okg.external_action_restore_attestations(deployment_name, envelope_digest) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS okg.external_action_restore_sessions (
  restore_session_row_id BIGSERIAL PRIMARY KEY, deployment_name TEXT NOT NULL, restore_id TEXT NOT NULL,
  request_hash TEXT NOT NULL, idempotency_key_hash TEXT NOT NULL,
  operator_separation_subject TEXT NOT NULL, authentication_audit_id TEXT NOT NULL,
  nomos_decision_id TEXT NOT NULL, restore_attestation_envelope_digest TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('quarantined','staging','complete','failed')),
  completion_receipt_envelope_digest TEXT, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), UNIQUE (deployment_name, restore_id),
  FOREIGN KEY (deployment_name, restore_attestation_envelope_digest)
    REFERENCES okg.external_action_restore_attestations(deployment_name, envelope_digest) ON DELETE RESTRICT,
  FOREIGN KEY (deployment_name, authentication_audit_id)
    REFERENCES okg.external_action_auth_audits(deployment_name, authentication_audit_id) ON DELETE RESTRICT,
  FOREIGN KEY (deployment_name, nomos_decision_id)
    REFERENCES okg.nomos_decisions(deployment_name, decision_id) ON DELETE RESTRICT
);

-- Restore staging sessions are immutable. Finalization is represented by an
-- append-only terminal receipt instead of rewriting the staging evidence.
CREATE TABLE IF NOT EXISTS okg.external_action_restore_completion_receipts (
  completion_receipt_row_id BIGSERIAL PRIMARY KEY,
  deployment_name TEXT NOT NULL,
  restore_id TEXT NOT NULL,
  restore_attestation_envelope_digest TEXT NOT NULL,
  completion_receipt_envelope_digest TEXT NOT NULL,
  completed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  authentication_audit_id TEXT NOT NULL,
  nomos_decision_id TEXT NOT NULL,
  store_record JSONB,
  protected_b64 TEXT,
  payload_b64 TEXT,
  signature_b64 TEXT,
  completion_receipt_projection JSONB,
  UNIQUE (deployment_name, restore_id),
  UNIQUE (deployment_name, completion_receipt_envelope_digest),
  FOREIGN KEY (deployment_name, restore_id)
    REFERENCES okg.external_action_restore_sessions(deployment_name, restore_id) ON DELETE RESTRICT,
  FOREIGN KEY (deployment_name, authentication_audit_id)
    REFERENCES okg.external_action_auth_audits(deployment_name, authentication_audit_id) ON DELETE RESTRICT,
  FOREIGN KEY (deployment_name, nomos_decision_id)
    REFERENCES okg.nomos_decisions(deployment_name, decision_id) ON DELETE RESTRICT
);
ALTER TABLE okg.external_action_restore_completion_receipts
  ADD COLUMN IF NOT EXISTS protected_b64 TEXT,
  ADD COLUMN IF NOT EXISTS payload_b64 TEXT,
  ADD COLUMN IF NOT EXISTS signature_b64 TEXT,
  ADD COLUMN IF NOT EXISTS completion_receipt_projection JSONB;

-- The API accepts signed envelope references and a checked projection of the
-- restore inventory.  Keep the projection on the immutable session so the
-- finalizer can compare staged evidence to an authoritative set rather than
-- treating caller-supplied counts as coverage.
ALTER TABLE okg.external_action_restore_sessions
  ADD COLUMN IF NOT EXISTS effect_inventory JSONB NOT NULL DEFAULT '[]'::jsonb,
  ADD COLUMN IF NOT EXISTS effect_inventory_digest TEXT,
  ADD COLUMN IF NOT EXISTS effect_inventory_count BIGINT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS old_epoch_lease_envelope_digest TEXT,
  ADD COLUMN IF NOT EXISTS new_epoch_lease_envelope_digest TEXT,
  ADD COLUMN IF NOT EXISTS worker_quiescence_envelope_digest TEXT,
  ADD COLUMN IF NOT EXISTS provider_authority_fence_envelope_digest TEXT,
  ADD COLUMN IF NOT EXISTS ledger_fence_envelope_digest TEXT;

CREATE TABLE IF NOT EXISTS okg.external_action_restore_finalize_idempotency (
  finalize_idempotency_row_id BIGSERIAL PRIMARY KEY,
  deployment_name TEXT NOT NULL,
  restore_id TEXT NOT NULL,
  operator_separation_subject TEXT NOT NULL,
  idempotency_key_hash TEXT NOT NULL,
  request_hash TEXT NOT NULL,
  response_body_jcs BYTEA NOT NULL,
  completion_receipt_envelope_digest TEXT NOT NULL,
  committed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (deployment_name, restore_id),
  UNIQUE (deployment_name, restore_id, operator_separation_subject, idempotency_key_hash),
  FOREIGN KEY (deployment_name, restore_id)
    REFERENCES okg.external_action_restore_sessions(deployment_name, restore_id)
    ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS okg.external_action_restore_checkpoint_staging (
  checkpoint_staging_row_id BIGSERIAL PRIMARY KEY, deployment_name TEXT NOT NULL,
  restore_id TEXT NOT NULL, batch_index BIGINT NOT NULL, request_hash TEXT NOT NULL,
  checkpoint_envelope_digest TEXT NOT NULL, proof_digest TEXT NOT NULL,
  checkpoint_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  effect_instance_key TEXT,
  operator_separation_subject TEXT,
  idempotency_key_hash TEXT,
  staged_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (deployment_name, restore_id, batch_index, request_hash),
  UNIQUE (deployment_name, restore_id, batch_index, checkpoint_envelope_digest),
  FOREIGN KEY (deployment_name, restore_id)
    REFERENCES okg.external_action_restore_sessions(deployment_name, restore_id) ON DELETE RESTRICT
);

ALTER TABLE okg.external_action_restore_checkpoint_staging
  ADD COLUMN IF NOT EXISTS effect_instance_key TEXT,
  ADD COLUMN IF NOT EXISTS operator_separation_subject TEXT,
  ADD COLUMN IF NOT EXISTS idempotency_key_hash TEXT;
CREATE UNIQUE INDEX IF NOT EXISTS external_action_restore_checkpoint_batch_idx
  ON okg.external_action_restore_checkpoint_staging (deployment_name, restore_id, batch_index);

-- A staging row is a batch receipt/header.  Checkpoints are separate
-- immutable rows so finalization counts entries and checks one-to-one
-- coverage rather than accidentally counting batches.
CREATE TABLE IF NOT EXISTS okg.external_action_restore_checkpoint_entries (
  checkpoint_entry_row_id BIGSERIAL PRIMARY KEY,
  deployment_name TEXT NOT NULL,
  restore_id TEXT NOT NULL,
  batch_index BIGINT NOT NULL CHECK (batch_index >= 0),
  ordinal BIGINT NOT NULL CHECK (ordinal >= 0),
  effect_instance_key TEXT NOT NULL,
  checkpoint_envelope_digest TEXT NOT NULL,
  proof_digest TEXT NOT NULL,
  leaf_index BIGINT CHECK (leaf_index >= 0),
  proof JSONB NOT NULL DEFAULT '{}'::jsonb,
  checkpoint_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  staged_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (deployment_name, restore_id, batch_index, ordinal),
  UNIQUE (deployment_name, restore_id, batch_index, effect_instance_key),
  UNIQUE (deployment_name, restore_id, checkpoint_envelope_digest),
  FOREIGN KEY (deployment_name, restore_id)
    REFERENCES okg.external_action_restore_sessions(deployment_name, restore_id)
    ON DELETE RESTRICT
);
ALTER TABLE okg.external_action_restore_checkpoint_entries
  ADD COLUMN IF NOT EXISTS leaf_index BIGINT,
  ADD COLUMN IF NOT EXISTS proof JSONB NOT NULL DEFAULT '{}'::jsonb;
DO $external_action_restore_checkpoint_leaf_index_check$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_catalog.pg_constraint
     WHERE conrelid='okg.external_action_restore_checkpoint_entries'::pg_catalog.regclass
       AND conname='external_action_restore_checkpoint_entries_leaf_index_check'
  ) THEN
    ALTER TABLE okg.external_action_restore_checkpoint_entries
      ADD CONSTRAINT external_action_restore_checkpoint_entries_leaf_index_check
      CHECK (leaf_index IS NULL OR leaf_index >= 0);
  END IF;
END
$external_action_restore_checkpoint_leaf_index_check$;

CREATE TABLE IF NOT EXISTS okg.external_action_restore_recoveries (
  recovery_row_id BIGSERIAL PRIMARY KEY, deployment_name TEXT NOT NULL, old_epoch BIGINT NOT NULL,
  new_epoch BIGINT NOT NULL, effect_instance_key TEXT NOT NULL, submission_content_hash TEXT NOT NULL,
  checkpoint_digest TEXT NOT NULL, restore_attestation_digest TEXT NOT NULL,
  classification TEXT NOT NULL CHECK (classification IN (
    'database_restore_no_effect_started','database_restore_terminal_review_confirmed',
    'database_restore_effect_uncertain','database_restore_succeeded','database_restore_failed',
    'database_restore_terminal_confirmed','database_restore_terminal_conflict')),
  source_proposal_id TEXT, source_decision_id TEXT,
  source_execution_id TEXT, recovered_review_state TEXT, recovered_execution_state TEXT,
  recovered_outcome TEXT, effective_at TIMESTAMPTZ NOT NULL,
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(), pre_restore_effect_event_head_hash TEXT,
  restored_effect_event_head_hash TEXT, signed_worker_checkpoint_digest TEXT NOT NULL,
  recovery_digest TEXT,
  UNIQUE (deployment_name, effect_instance_key, signed_worker_checkpoint_digest),
  FOREIGN KEY (deployment_name, restore_attestation_digest)
    REFERENCES okg.external_action_restore_attestations(deployment_name, envelope_digest) ON DELETE RESTRICT,
  FOREIGN KEY (deployment_name, source_proposal_id)
    REFERENCES okg.external_action_proposals(deployment_name, proposal_id) ON DELETE RESTRICT,
  FOREIGN KEY (deployment_name, source_decision_id)
    REFERENCES okg.external_action_decisions(deployment_name, decision_id) ON DELETE RESTRICT,
  FOREIGN KEY (source_execution_id)
    REFERENCES okg.external_action_executions(execution_id) ON DELETE RESTRICT
);
ALTER TABLE okg.external_action_restore_recoveries
  ADD COLUMN IF NOT EXISTS recovery_digest TEXT;
DO $external_action_restore_recovery_classification_check$
BEGIN
  ALTER TABLE okg.external_action_restore_recoveries
    DROP CONSTRAINT IF EXISTS external_action_restore_recoveries_classification_check;
  ALTER TABLE okg.external_action_restore_recoveries
    ADD CONSTRAINT external_action_restore_recoveries_classification_check
    CHECK (classification IN (
      'database_restore_no_effect_started','database_restore_terminal_review_confirmed',
      'database_restore_effect_uncertain','database_restore_succeeded','database_restore_failed',
      'database_restore_terminal_confirmed','database_restore_terminal_conflict'));
END
$external_action_restore_recovery_classification_check$;

CREATE TABLE IF NOT EXISTS okg.external_action_api_role_bindings (
  "session_user" TEXT PRIMARY KEY CHECK (btrim("session_user") <> ''),
  deployment_name TEXT NOT NULL CHECK (btrim(deployment_name) <> ''),
  binding_version BIGINT NOT NULL DEFAULT 1, status TEXT NOT NULL DEFAULT 'active'
    CHECK (status IN ('active','disabled')), bound_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  verifier_capability_digest TEXT,
  verifier_capability_generation BIGINT NOT NULL DEFAULT 1,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), UNIQUE ("session_user", deployment_name)
);
ALTER TABLE okg.external_action_api_role_bindings
  ADD COLUMN IF NOT EXISTS verifier_capability_digest TEXT,
  ADD COLUMN IF NOT EXISTS verifier_capability_generation BIGINT NOT NULL DEFAULT 1;

-- The adapter keeps a JSON-shaped copy of each validated domain record for
-- exact restart hydration.  The normalized columns above remain the query and
-- constraint surface; these private columns are absent from API projections.
ALTER TABLE okg.external_action_proposals
  ADD COLUMN IF NOT EXISTS store_record JSONB,
  ADD COLUMN IF NOT EXISTS action_contract_record JSONB,
  ADD COLUMN IF NOT EXISTS submission_idempotency_record JSONB;
ALTER TABLE okg.external_action_payload_versions
  ADD COLUMN IF NOT EXISTS store_record JSONB;
ALTER TABLE okg.external_action_auth_audits
  ADD COLUMN IF NOT EXISTS store_record JSONB,
  ADD COLUMN IF NOT EXISTS provider_status TEXT,
  ADD COLUMN IF NOT EXISTS binding_status TEXT,
  ADD COLUMN IF NOT EXISTS provider_updated_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS binding_updated_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS provider_credential_generation_digest TEXT,
  ADD COLUMN IF NOT EXISTS binding_credential_generation_digest TEXT,
  ADD COLUMN IF NOT EXISTS credential_generation_digest TEXT,
  ADD COLUMN IF NOT EXISTS presented_credential_generation_digest TEXT,
  ADD COLUMN IF NOT EXISTS token_issued_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS certificate_thumbprint TEXT,
  ADD COLUMN IF NOT EXISTS certificate_not_before TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS certificate_not_after TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS verifier_assertion_nonce_digest TEXT,
  ADD COLUMN IF NOT EXISTS verifier_assertion_expires_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS verifier_transition_digest TEXT,
  ADD COLUMN IF NOT EXISTS verifier_policy_input_digest TEXT,
  ADD COLUMN IF NOT EXISTS verifier_required_obligations_digest TEXT,
  ADD COLUMN IF NOT EXISTS verifier_completed_obligations_digest TEXT,
  ADD COLUMN IF NOT EXISTS verifier_missing_obligations_digest TEXT,
  ADD COLUMN IF NOT EXISTS verifier_assertion_tag TEXT,
  ADD COLUMN IF NOT EXISTS verifier_capability_generation BIGINT;
CREATE UNIQUE INDEX IF NOT EXISTS external_action_auth_audits_verifier_nonce_idx
  ON okg.external_action_auth_audits
    (deployment_name, verifier_capability_generation, verifier_assertion_nonce_digest)
  WHERE verifier_assertion_nonce_digest IS NOT NULL;

DO $external_action_auth_audit_checks$
BEGIN
  ALTER TABLE okg.external_action_api_role_bindings
    DROP CONSTRAINT IF EXISTS external_action_api_role_bindings_verifier_check;
  ALTER TABLE okg.external_action_api_role_bindings
    ADD CONSTRAINT external_action_api_role_bindings_verifier_check CHECK (
      verifier_capability_generation > 0
      AND (
        verifier_capability_digest IS NULL
        OR verifier_capability_digest ~ '^sha256:[0-9a-f]{64}$'
      )
    );
  ALTER TABLE okg.external_action_auth_audits
    DROP CONSTRAINT IF EXISTS external_action_auth_audits_provider_status_check;
  ALTER TABLE okg.external_action_auth_audits
    ADD CONSTRAINT external_action_auth_audits_provider_status_check
    CHECK (provider_status IS NULL OR provider_status IN ('active','disabled','rotating','revoked'));
  ALTER TABLE okg.external_action_auth_audits
    DROP CONSTRAINT IF EXISTS external_action_auth_audits_binding_status_check;
  ALTER TABLE okg.external_action_auth_audits
    ADD CONSTRAINT external_action_auth_audits_binding_status_check
    CHECK (binding_status IS NULL OR binding_status IN ('active','disabled'));
  ALTER TABLE okg.external_action_auth_audits
    DROP CONSTRAINT IF EXISTS external_action_auth_audits_provider_credential_generatio_check;
  ALTER TABLE okg.external_action_auth_audits
    DROP CONSTRAINT IF EXISTS external_action_auth_audits_provider_generation_check;
  ALTER TABLE okg.external_action_auth_audits
    ADD CONSTRAINT external_action_auth_audits_provider_generation_check
    CHECK (provider_credential_generation_digest IS NULL OR provider_credential_generation_digest ~ '^sha256:[0-9a-f]{64}$');
  ALTER TABLE okg.external_action_auth_audits
    DROP CONSTRAINT IF EXISTS external_action_auth_audits_binding_credential_generation_check;
  ALTER TABLE okg.external_action_auth_audits
    DROP CONSTRAINT IF EXISTS external_action_auth_audits_binding_generation_check;
  ALTER TABLE okg.external_action_auth_audits
    ADD CONSTRAINT external_action_auth_audits_binding_generation_check
    CHECK (binding_credential_generation_digest IS NULL OR binding_credential_generation_digest ~ '^sha256:[0-9a-f]{64}$');
  ALTER TABLE okg.external_action_auth_audits
    DROP CONSTRAINT IF EXISTS external_action_auth_audits_credential_generation_digest_check;
  ALTER TABLE okg.external_action_auth_audits
    DROP CONSTRAINT IF EXISTS external_action_auth_audits_credential_generation_check;
  ALTER TABLE okg.external_action_auth_audits
    ADD CONSTRAINT external_action_auth_audits_credential_generation_check
    CHECK (credential_generation_digest IS NULL OR credential_generation_digest ~ '^sha256:[0-9a-f]{64}$');
  ALTER TABLE okg.external_action_auth_audits
    DROP CONSTRAINT IF EXISTS external_action_auth_audits_presented_credential_generati_check;
  ALTER TABLE okg.external_action_auth_audits
    DROP CONSTRAINT IF EXISTS external_action_auth_audits_presented_generation_check;
  ALTER TABLE okg.external_action_auth_audits
    ADD CONSTRAINT external_action_auth_audits_presented_generation_check
    CHECK (presented_credential_generation_digest IS NULL OR presented_credential_generation_digest ~ '^sha256:[0-9a-f]{64}$');
  ALTER TABLE okg.external_action_auth_audits
    DROP CONSTRAINT IF EXISTS external_action_auth_audits_certificate_thumbprint_check;
  ALTER TABLE okg.external_action_auth_audits
    ADD CONSTRAINT external_action_auth_audits_certificate_thumbprint_check
    CHECK (certificate_thumbprint IS NULL OR (btrim(certificate_thumbprint) = certificate_thumbprint AND octet_length(certificate_thumbprint) BETWEEN 1 AND 512));
  ALTER TABLE okg.external_action_auth_audits
    DROP CONSTRAINT IF EXISTS external_action_auth_audits_certificate_interval_check;
  ALTER TABLE okg.external_action_auth_audits
    ADD CONSTRAINT external_action_auth_audits_certificate_interval_check
    CHECK (certificate_not_before IS NULL OR certificate_not_after IS NULL OR certificate_not_after >= certificate_not_before);
END
$external_action_auth_audit_checks$;

CREATE OR REPLACE FUNCTION okg.external_action_api_role_binding_guard()
RETURNS TRIGGER
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $external_action_api_role_binding_guard$
BEGIN
  IF TG_OP = 'INSERT' THEN
    IF NEW.binding_version <> 1
       OR NEW.verifier_capability_generation <> 1 THEN
      RAISE EXCEPTION 'external action API binding must start at version and capability generation one'
        USING ERRCODE = '22023';
    END IF;
    RETURN NEW;
  END IF;
  IF NEW."session_user" IS DISTINCT FROM OLD."session_user"
     OR NEW.deployment_name IS DISTINCT FROM OLD.deployment_name
     OR NEW.binding_version IS DISTINCT FROM OLD.binding_version
     OR NEW.bound_at IS DISTINCT FROM OLD.bound_at THEN
    RAISE EXCEPTION 'external action API binding identity is immutable'
      USING ERRCODE = '55006';
  END IF;
  IF OLD.verifier_capability_digest IS NULL
     AND NEW.verifier_capability_digest IS NOT NULL THEN
    IF NEW.verifier_capability_generation <> OLD.verifier_capability_generation THEN
      RAISE EXCEPTION 'initial external action verifier provisioning cannot change generation'
        USING ERRCODE = '22023';
    END IF;
  ELSIF NEW.verifier_capability_digest IS DISTINCT FROM
        OLD.verifier_capability_digest THEN
    IF NEW.verifier_capability_digest IS NULL
       OR NEW.verifier_capability_generation <>
            OLD.verifier_capability_generation + 1 THEN
      RAISE EXCEPTION 'external action verifier rotation must replace the digest and advance one generation'
        USING ERRCODE = '22023';
    END IF;
  ELSIF NEW.verifier_capability_generation IS DISTINCT FROM
        OLD.verifier_capability_generation THEN
    RAISE EXCEPTION 'external action verifier generation requires a new capability digest'
      USING ERRCODE = '22023';
  END IF;
  NEW.updated_at := clock_timestamp();
  RETURN NEW;
END
$external_action_api_role_binding_guard$;
REVOKE ALL ON FUNCTION okg.external_action_api_role_binding_guard() FROM PUBLIC;
DROP TRIGGER IF EXISTS external_action_api_role_binding_guard_trigger
  ON okg.external_action_api_role_bindings;
CREATE TRIGGER external_action_api_role_binding_guard_trigger
BEFORE INSERT OR UPDATE ON okg.external_action_api_role_bindings
FOR EACH ROW EXECUTE FUNCTION okg.external_action_api_role_binding_guard();

ALTER TABLE okg.external_action_decisions
  ADD COLUMN IF NOT EXISTS store_record JSONB;
ALTER TABLE okg.external_action_transition_idempotency
  ADD COLUMN IF NOT EXISTS store_record JSONB;
ALTER TABLE okg.external_action_executions
  ADD COLUMN IF NOT EXISTS store_record JSONB;
ALTER TABLE okg.external_action_owner_unavailability_attestations
  ADD COLUMN IF NOT EXISTS store_record JSONB;
ALTER TABLE okg.external_action_outcomes
  ADD COLUMN IF NOT EXISTS store_record JSONB;
ALTER TABLE okg.external_action_events
  ADD COLUMN IF NOT EXISTS store_record JSONB;
ALTER TABLE okg.external_action_execution_epoch_leases
  ADD COLUMN IF NOT EXISTS store_record JSONB;
ALTER TABLE okg.external_action_restore_attestations
  ADD COLUMN IF NOT EXISTS store_record JSONB;
ALTER TABLE okg.external_action_restore_fences
  ADD COLUMN IF NOT EXISTS store_record JSONB;
ALTER TABLE okg.external_action_restore_sessions
  ADD COLUMN IF NOT EXISTS store_record JSONB;
ALTER TABLE okg.external_action_restore_checkpoint_staging
  ADD COLUMN IF NOT EXISTS store_record JSONB;
ALTER TABLE okg.external_action_restore_recoveries
  ADD COLUMN IF NOT EXISTS store_record JSONB;

CREATE INDEX IF NOT EXISTS external_action_proposals_review_idx
  ON okg.external_action_proposals (deployment_name, review_state, updated_at DESC);
CREATE INDEX IF NOT EXISTS external_action_events_effect_idx
  ON okg.external_action_events (deployment_name, effect_instance_key, sequence);
CREATE INDEX IF NOT EXISTS external_action_api_role_bindings_deployment_idx
  ON okg.external_action_api_role_bindings (deployment_name, status);
CREATE INDEX IF NOT EXISTS external_action_auth_audits_binding_idx
  ON okg.external_action_auth_audits
    (deployment_name, nomos_provider_id, nomos_subject_binding_id, observed_at DESC);

CREATE OR REPLACE FUNCTION okg.external_action_reject_mutation()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog, okg AS $$
BEGIN
  RAISE EXCEPTION 'external action relation % is append-only', TG_TABLE_NAME
    USING ERRCODE = '55006';
END
$$;

DO $external_action_append_only_triggers$
DECLARE relation_name TEXT;
BEGIN
  FOREACH relation_name IN ARRAY ARRAY[
    'external_action_payload_versions','external_action_decisions',
    'external_action_transition_idempotency','external_action_executions',
    'external_action_outcomes','external_action_events','external_action_auth_audits',
    'external_action_dpop_replay_nonces','external_action_owner_unavailability_attestations',
    'external_action_execution_epoch_leases','external_action_restore_attestations',
    'external_action_restore_sessions','external_action_restore_checkpoint_staging',
    'external_action_restore_completion_receipts',
    'external_action_restore_finalize_idempotency',
    'external_action_restore_checkpoint_entries',
    'external_action_restore_recoveries'
  ] LOOP
    EXECUTE format('DROP TRIGGER IF EXISTS %I_append_only ON okg.%I', relation_name, relation_name);
    EXECUTE format('CREATE TRIGGER %I_append_only BEFORE UPDATE OR DELETE ON okg.%I FOR EACH STATEMENT EXECUTE FUNCTION okg.external_action_reject_mutation()', relation_name, relation_name);
  END LOOP;
END
$external_action_append_only_triggers$;

CREATE OR REPLACE FUNCTION okg.external_action_monotonic_nomos_updated_at()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog, okg AS $$
BEGIN
  -- `now()` is transaction-stable, so disable -> re-enable in one
  -- transaction could otherwise reuse the pre-rotation barrier.  Advance
  -- from the old value by one microsecond and use the wall clock as the
  -- floor; every committed identity mutation therefore invalidates older
  -- credentials, even when several mutations share a transaction.
  NEW.updated_at := GREATEST(
    COALESCE(OLD.updated_at, '-infinity'::timestamptz) + interval '1 microsecond',
    clock_timestamp()
  );
  RETURN NEW;
END
$$;

DROP TRIGGER IF EXISTS external_action_identity_provider_updated_at ON okg.nomos_identity_providers;
CREATE TRIGGER external_action_identity_provider_updated_at
  BEFORE UPDATE ON okg.nomos_identity_providers FOR EACH ROW
  EXECUTE FUNCTION okg.external_action_monotonic_nomos_updated_at();
DROP TRIGGER IF EXISTS external_action_subject_binding_updated_at ON okg.nomos_subject_bindings;
CREATE TRIGGER external_action_subject_binding_updated_at
  BEFORE UPDATE ON okg.nomos_subject_bindings FOR EACH ROW
  EXECUTE FUNCTION okg.external_action_monotonic_nomos_updated_at();
DROP TRIGGER IF EXISTS external_action_policy_bundle_updated_at ON okg.nomos_policy_bundles;
CREATE TRIGGER external_action_policy_bundle_updated_at
  BEFORE UPDATE ON okg.nomos_policy_bundles FOR EACH ROW
  EXECUTE FUNCTION okg.external_action_monotonic_nomos_updated_at();

-- Policy and identity mutations share the deployment authority lock with
-- lifecycle commands.  A transition therefore cannot validate an allow and
-- commit across a concurrent authority change.
CREATE OR REPLACE FUNCTION okg.external_action_nomos_authority_lock()
RETURNS TRIGGER
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $external_action_nomos_authority_lock$
DECLARE
  mutation_deployment TEXT;
BEGIN
  IF TG_OP = 'DELETE' THEN
    mutation_deployment := OLD.deployment_name;
  ELSE
    mutation_deployment := NEW.deployment_name;
  END IF;
  IF mutation_deployment IS NULL OR btrim(mutation_deployment) = '' THEN
    RAISE EXCEPTION 'Nomos authority mutation has no deployment'
      USING ERRCODE = '22023';
  END IF;
  IF TG_OP = 'UPDATE'
     AND OLD.deployment_name IS DISTINCT FROM NEW.deployment_name THEN
    RAISE EXCEPTION 'Nomos authority deployment is immutable'
      USING ERRCODE = '22023';
  END IF;
  IF TG_OP = 'UPDATE'
     AND TG_TABLE_NAME = 'nomos_policy_bundles'
     AND (
       to_jsonb(OLD) - ARRAY[
         'status','effective_from','effective_until','metadata','updated_at'
       ]::text[]
     ) IS DISTINCT FROM (
       to_jsonb(NEW) - ARRAY[
         'status','effective_from','effective_until','metadata','updated_at'
       ]::text[]
     ) THEN
    RAISE EXCEPTION 'Nomos policy content is immutable for a retained fingerprint'
      USING ERRCODE = '22023';
  END IF;
  PERFORM pg_advisory_xact_lock(
    hashtextextended(
      'okg.external_action.deployment:' || mutation_deployment, 0
    )
  );
  IF TG_OP = 'DELETE' THEN
    RETURN OLD;
  END IF;
  RETURN NEW;
END
$external_action_nomos_authority_lock$;

DROP TRIGGER IF EXISTS external_action_nomos_policy_bundle_authority_lock
  ON okg.nomos_policy_bundles;
CREATE TRIGGER external_action_nomos_policy_bundle_authority_lock
  BEFORE INSERT OR UPDATE OR DELETE ON okg.nomos_policy_bundles
  FOR EACH ROW EXECUTE FUNCTION okg.external_action_nomos_authority_lock();
DROP TRIGGER IF EXISTS external_action_nomos_policy_activation_authority_lock
  ON okg.nomos_policy_activations;
CREATE TRIGGER external_action_nomos_policy_activation_authority_lock
  BEFORE INSERT OR UPDATE OR DELETE ON okg.nomos_policy_activations
  FOR EACH ROW EXECUTE FUNCTION okg.external_action_nomos_authority_lock();
DROP TRIGGER IF EXISTS external_action_nomos_identity_provider_authority_lock
  ON okg.nomos_identity_providers;
CREATE TRIGGER external_action_nomos_identity_provider_authority_lock
  BEFORE INSERT OR UPDATE OR DELETE ON okg.nomos_identity_providers
  FOR EACH ROW EXECUTE FUNCTION okg.external_action_nomos_authority_lock();
DROP TRIGGER IF EXISTS external_action_nomos_subject_binding_authority_lock
  ON okg.nomos_subject_bindings;
CREATE TRIGGER external_action_nomos_subject_binding_authority_lock
  BEFORE INSERT OR UPDATE OR DELETE ON okg.nomos_subject_bindings
  FOR EACH ROW EXECUTE FUNCTION okg.external_action_nomos_authority_lock();

CREATE OR REPLACE FUNCTION okg.external_action_session_deployment()
RETURNS TEXT LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, okg AS $$
DECLARE resolved_session_user TEXT := session_user; resolved_deployment TEXT; binding_count INTEGER;
BEGIN
  SELECT count(*)::integer, min(b.deployment_name) INTO binding_count, resolved_deployment
    FROM okg.external_action_api_role_bindings b
   WHERE b."session_user" = resolved_session_user AND b.status = 'active';
  IF binding_count <> 1 OR resolved_deployment IS NULL THEN
    RAISE EXCEPTION 'external action API login has no unique active deployment binding' USING ERRCODE = '28000';
  END IF;
  RETURN resolved_deployment;
END
$$;

-- Startup hashes bind to the database's system identifier and the exact
-- session-user row selected under a row lock.  Callers cannot substitute a
-- login or deployment projection from configuration.
CREATE OR REPLACE FUNCTION okg.external_action_startup_login_binding()
RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $external_action_startup_login_binding$
DECLARE
  resolved_session_user TEXT := session_user;
  bound_deployment TEXT := okg.external_action_session_deployment();
  binding_row okg.external_action_api_role_bindings%ROWTYPE;
  system_identifier TEXT;
BEGIN
  SELECT * INTO binding_row
    FROM okg.external_action_api_role_bindings
   WHERE "session_user" = resolved_session_user
     AND deployment_name = bound_deployment
     AND status = 'active'
   FOR SHARE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'external action API login has no active deployment binding'
      USING ERRCODE = '28000';
  END IF;
  SELECT c.system_identifier::TEXT INTO system_identifier
    FROM pg_control_system() AS c;
  RETURN jsonb_build_object(
    'database_system_identifier', system_identifier,
    'session_user', binding_row."session_user",
    'deployment', binding_row.deployment_name,
    'binding_version', binding_row.binding_version,
    'verifier_capability_generation', binding_row.verifier_capability_generation,
    'bound_at', to_char(
      binding_row.bound_at AT TIME ZONE 'UTC',
      'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
    )
  );
END
$external_action_startup_login_binding$;
REVOKE ALL ON FUNCTION okg.external_action_startup_login_binding() FROM PUBLIC;

-- Return only the database-owned claim-owner availability projection used by
-- the service preflight.  The execution's retained authentication audit is
-- the identity source; callers cannot provide provider or subject fields.
CREATE OR REPLACE FUNCTION okg.external_action_claim_owner_unavailability(p_execution_id TEXT)
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, okg AS $external_action_claim_owner_unavailability$
DECLARE
  bound_deployment TEXT := okg.external_action_session_deployment(); observed_at TIMESTAMPTZ := clock_timestamp();
  execution_row okg.external_action_executions%ROWTYPE;
  claim_auth_provider_id TEXT; claim_auth_binding_id TEXT; claim_auth_subject TEXT;
  claim_provider_status TEXT; claim_provider_updated_at TIMESTAMPTZ;
  claim_binding_status TEXT; claim_binding_updated_at TIMESTAMPTZ; claim_binding_subject TEXT;
  revocation_updated_at TIMESTAMPTZ; active_replacement BOOLEAN; unavailable BOOLEAN;
BEGIN
  PERFORM okg.external_action_verifier_capability_status();
  IF p_execution_id IS NULL OR btrim(p_execution_id)='' THEN RAISE EXCEPTION 'external action execution id is required' USING ERRCODE='22023'; END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended('okg.external_action.deployment:'||bound_deployment,0));
  SELECT * INTO execution_row FROM okg.external_action_executions WHERE deployment_name=bound_deployment AND execution_id=p_execution_id FOR SHARE;
  IF execution_row.execution_id IS NULL THEN RAISE EXCEPTION 'external action execution was not found' USING ERRCODE='P0002'; END IF;
  SELECT a.nomos_provider_id,a.nomos_subject_binding_id,a.separation_subject,p.status,p.updated_at,b.status,b.updated_at,b.subject
    INTO claim_auth_provider_id,claim_auth_binding_id,claim_auth_subject,claim_provider_status,claim_provider_updated_at,claim_binding_status,claim_binding_updated_at,claim_binding_subject
    FROM okg.external_action_auth_audits a
    JOIN okg.nomos_identity_providers p ON p.deployment_name=a.deployment_name AND p.provider_id=a.nomos_provider_id
    JOIN okg.nomos_subject_bindings b ON b.deployment_name=a.deployment_name AND b.binding_id=a.nomos_subject_binding_id AND b.provider_id=p.provider_id AND b.issuer=p.issuer AND b.subject=a.separation_subject
   WHERE a.deployment_name=bound_deployment AND a.authentication_audit_id=execution_row.authentication_audit_id AND a.separation_subject=execution_row.worker_separation_subject;
  IF claim_auth_provider_id IS NULL OR claim_auth_binding_id IS NULL OR claim_auth_subject IS DISTINCT FROM execution_row.worker_separation_subject OR claim_binding_subject IS DISTINCT FROM execution_row.worker_separation_subject THEN RAISE EXCEPTION 'claim owner identity authority is unavailable' USING ERRCODE='42501'; END IF;
  active_replacement := EXISTS (SELECT 1 FROM okg.nomos_identity_providers p JOIN okg.nomos_subject_bindings b ON b.deployment_name=p.deployment_name AND b.provider_id=p.provider_id AND b.issuer=p.issuer WHERE p.deployment_name=bound_deployment AND b.subject=execution_row.worker_separation_subject AND NOT (p.provider_id=claim_auth_provider_id AND b.binding_id=claim_auth_binding_id) AND p.status='active' AND b.status='active' AND (p.effective_from IS NULL OR p.effective_from<=observed_at) AND (p.effective_until IS NULL OR observed_at<p.effective_until) AND (b.effective_from IS NULL OR b.effective_from<=observed_at) AND (b.effective_until IS NULL OR observed_at<b.effective_until));
  revocation_updated_at := CASE WHEN claim_provider_status IN ('disabled','revoked') AND claim_provider_updated_at>execution_row.claimed_at THEN claim_provider_updated_at WHEN claim_binding_status IN ('disabled','revoked') AND claim_binding_updated_at>execution_row.claimed_at THEN claim_binding_updated_at ELSE GREATEST(claim_provider_updated_at,claim_binding_updated_at) END;
  unavailable := ((claim_provider_status IN ('disabled','revoked') AND claim_provider_updated_at>execution_row.claimed_at) OR (claim_binding_status IN ('disabled','revoked') AND claim_binding_updated_at>execution_row.claimed_at)) AND NOT active_replacement;
  RETURN jsonb_build_object('authentication_audit_id',execution_row.authentication_audit_id,'unavailable',unavailable,'updated_at',to_char(revocation_updated_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),'active_replacement',active_replacement);
END
$external_action_claim_owner_unavailability$;
REVOKE ALL ON FUNCTION okg.external_action_claim_owner_unavailability(TEXT) FROM PUBLIC;

CREATE OR REPLACE FUNCTION okg.external_action_assert_deployment(p_deployment TEXT)
RETURNS BOOLEAN LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, okg AS $$
DECLARE bound_deployment TEXT;
BEGIN
  bound_deployment := okg.external_action_session_deployment();
  IF p_deployment IS NULL OR p_deployment <> bound_deployment THEN
    RAISE EXCEPTION 'external action deployment is not bound to session_user' USING ERRCODE = '28000';
  END IF;
  RETURN TRUE;
END
$$;

-- The standalone review service is deliberately not granted raw-table access.
-- These fixed entry points are the only write/read bridge used by its
-- Postgres adapter.  ``p_kind`` is a closed enum below; it is never a table
-- name and is never interpolated into SQL.  The function owner is provisioned
-- by views.sql; the conditional grants make this migration safe when it is
-- replayed before that role is created.
-- Fixed semantic command boundary.  The JSON command is a value, never a
-- relation name or SQL fragment.  Every public command validates its closed
-- object shape before reading any field; all SQL below is static.
CREATE OR REPLACE FUNCTION okg.external_action_require_command(
  p_command JSONB,
  p_allowed TEXT[],
  p_required TEXT[]
) RETURNS VOID
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $external_action_require_command$
DECLARE
  command_key TEXT;
  required_key TEXT;
  audit_row okg.external_action_auth_audits%ROWTYPE;
  actor_subject TEXT;
  actor_principal TEXT;
BEGIN
  IF jsonb_typeof(p_command) IS DISTINCT FROM 'object' THEN
    RAISE EXCEPTION 'external action command must be a JSON object'
      USING ERRCODE = '22023';
  END IF;
  FOR command_key IN SELECT jsonb_object_keys(p_command) LOOP
    IF NOT (command_key = ANY (p_allowed)) THEN
      RAISE EXCEPTION 'external action command contains an unknown field'
        USING ERRCODE = '22023';
    END IF;
  END LOOP;
  FOREACH required_key IN ARRAY p_required LOOP
    IF NOT (p_command ? required_key)
       OR p_command -> required_key IS NULL
       OR p_command -> required_key = 'null'::jsonb THEN
      RAISE EXCEPTION 'external action command is missing a required field'
        USING ERRCODE = '22023';
    END IF;
  END LOOP;
  -- Authenticated lifecycle actors are projections of the retained audit,
  -- never claims supplied by a mapped login.  Auth-audit creation itself is
  -- the one command that necessarily precedes this lookup.
  IF p_command ? 'authentication_audit_id'
     AND p_command->>'authentication_audit_id' IS NOT NULL
     AND NOT (p_command ? 'request_id') THEN
    SELECT * INTO audit_row
      FROM okg.external_action_auth_audits
     WHERE deployment_name = okg.external_action_session_deployment()
       AND authentication_audit_id = p_command->>'authentication_audit_id';
    IF audit_row.authentication_audit_id IS NULL THEN
      RAISE EXCEPTION 'authentication audit is unavailable for lifecycle actor'
        USING ERRCODE = '28000';
    END IF;
    FOREACH actor_subject IN ARRAY ARRAY[
      p_command->>'proposer_separation_subject',
      p_command->>'actor_separation_subject',
      p_command->>'worker_separation_subject',
      p_command->>'worker_or_reconciler_separation_subject',
      p_command->>'operator_separation_subject'
    ] LOOP
      IF actor_subject IS NOT NULL
         AND actor_subject IS DISTINCT FROM audit_row.separation_subject THEN
        RAISE EXCEPTION 'lifecycle actor is not the authenticated separation subject'
          USING ERRCODE = '42501';
      END IF;
    END LOOP;
    FOREACH actor_principal IN ARRAY ARRAY[
      p_command->>'proposer_principal',
      p_command->>'actor_principal',
      p_command->>'worker_principal'
    ] LOOP
      IF actor_principal IS NOT NULL
         AND actor_principal IS DISTINCT FROM audit_row.principal THEN
        RAISE EXCEPTION 'lifecycle principal is not the authenticated principal'
          USING ERRCODE = '42501';
      END IF;
    END LOOP;
  END IF;
END
$external_action_require_command$;

CREATE OR REPLACE FUNCTION okg.external_action_json_bytes(p_value JSONB)
RETURNS BYTEA
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $external_action_json_bytes$
BEGIN
  IF p_value IS NULL OR p_value = 'null'::jsonb THEN
    RETURN NULL;
  ELSIF jsonb_typeof(p_value) = 'object'
        AND p_value ? '__okg_bytes_b64' THEN
    RETURN decode(p_value ->> '__okg_bytes_b64', 'base64');
  ELSIF jsonb_typeof(p_value) = 'string' THEN
    RETURN convert_to(p_value #>> '{}', 'utf8');
  END IF;
  RETURN convert_to(p_value::text, 'utf8');
END
$external_action_json_bytes$;

-- Hash only a caller-independent, closed JSON projection.  The separator is
-- part of the contract and keeps adjacent domains unambiguous.  The
-- projection builders below include every closed event/decision field,
-- including JSON null for absent optional values.
CREATE OR REPLACE FUNCTION okg.external_action_hash(p_domain TEXT, p_value JSONB)
RETURNS TEXT
LANGUAGE SQL IMMUTABLE PARALLEL SAFE
SET search_path = pg_catalog, okg
AS $external_action_hash$
  SELECT 'sha256:' || encode(
    sha256(
      convert_to(p_domain, 'UTF8')
      || decode('00', 'hex')
      || convert_to(okg.jsonb_canonical_text(p_value), 'UTF8')
    ),
    'hex'
  )
$external_action_hash$;
REVOKE ALL ON FUNCTION okg.external_action_hash(TEXT, JSONB) FROM PUBLIC;

-- The external-action HTTP verifier holds a random 256-bit capability which
-- is independent of the mapped PostgreSQL login. Only its SHA-256 digest and
-- generation are retained in the deployment login binding. The raw value is
-- supplied as a transaction-local parameter and is never returned or stored.
CREATE OR REPLACE FUNCTION okg.external_action_hmac_sha256(
  p_key BYTEA,
  p_message BYTEA
) RETURNS TEXT
LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE
SET search_path = pg_catalog, okg
AS $external_action_hmac_sha256$
DECLARE
  key_block BYTEA;
  inner_pad BYTEA;
  outer_pad BYTEA;
  index_value INTEGER;
BEGIN
  IF octet_length(p_key) > 64 THEN
    p_key := sha256(p_key);
  END IF;
  key_block := p_key || decode(repeat('00', 64 - octet_length(p_key)), 'hex');
  inner_pad := key_block;
  outer_pad := key_block;
  FOR index_value IN 0..63 LOOP
    inner_pad := set_byte(
      inner_pad, index_value, get_byte(key_block, index_value) # 54
    );
    outer_pad := set_byte(
      outer_pad, index_value, get_byte(key_block, index_value) # 92
    );
  END LOOP;
  RETURN 'sha256:' || encode(
    sha256(outer_pad || sha256(inner_pad || p_message)), 'hex'
  );
END
$external_action_hmac_sha256$;
REVOKE ALL ON FUNCTION okg.external_action_hmac_sha256(BYTEA, BYTEA) FROM PUBLIC;

CREATE OR REPLACE FUNCTION okg.external_action_verifier_capability_status()
RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $external_action_verifier_capability_status$
DECLARE
  bound_deployment TEXT := okg.external_action_session_deployment();
  binding_row okg.external_action_api_role_bindings%ROWTYPE;
  capability_value TEXT := current_setting(
    'okg.external_action_verifier_capability', true
  );
  capability_digest TEXT;
BEGIN
  SELECT * INTO binding_row
    FROM okg.external_action_api_role_bindings
   WHERE "session_user" = session_user
     AND deployment_name = bound_deployment
     AND status = 'active'
   FOR SHARE;
  IF binding_row."session_user" IS NULL
     OR capability_value IS NULL
     OR capability_value !~ '^[0-9a-f]{64}$' THEN
    RAISE EXCEPTION 'external action verifier capability is unavailable'
      USING ERRCODE = '28000';
  END IF;
  capability_digest := 'sha256:' || encode(
    sha256(decode(capability_value, 'hex')), 'hex'
  );
  IF binding_row.verifier_capability_digest IS DISTINCT FROM capability_digest
     OR binding_row.verifier_capability_generation IS NULL
     OR binding_row.verifier_capability_generation <= 0 THEN
    RAISE EXCEPTION 'external action verifier capability is unavailable'
      USING ERRCODE = '28000';
  END IF;
  RETURN jsonb_build_object(
    'deployment', bound_deployment,
    'capability_generation', binding_row.verifier_capability_generation
  );
END
$external_action_verifier_capability_status$;
REVOKE ALL ON FUNCTION okg.external_action_verifier_capability_status() FROM PUBLIC;

CREATE OR REPLACE FUNCTION okg.external_action_verify_verifier_assertion(
  p_assertion JSONB,
  p_assertion_tag TEXT,
  p_expected JSONB
) RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $external_action_verify_verifier_assertion$
DECLARE
  capability_status JSONB;
  capability_value TEXT := current_setting(
    'okg.external_action_verifier_capability', true
  );
  expected_tag TEXT;
  expected_tag_bytes BYTEA;
  supplied_tag_bytes BYTEA;
  tag_difference INTEGER := 0;
  tag_index INTEGER;
  issued_value TIMESTAMPTZ;
  expires_value TIMESTAMPTZ;
  observed_at TIMESTAMPTZ := clock_timestamp();
  digest_key TEXT;
BEGIN
  IF jsonb_typeof(p_assertion) IS DISTINCT FROM 'object'
     OR EXISTS (
       SELECT 1 FROM jsonb_object_keys(p_assertion) AS key_name
        WHERE key_name NOT IN (
          'schema','deployment','capability_generation',
          'authentication_audit_id','operation','transition_digest',
          'credential_evidence_digest','nonce_digest','issued_at','expires_at',
          'nomos_decision_id','policy_fingerprint','provider_id',
          'provider_status','provider_updated_at','binding_id','binding_status',
          'binding_updated_at','nomos_input_hash','allowed','final_effect',
          'required_obligations_digest','completed_obligations_digest',
          'missing_obligations_digest'
        )
     )
     OR EXISTS (
       SELECT 1 FROM unnest(ARRAY[
         'schema','deployment','capability_generation',
         'authentication_audit_id','operation','transition_digest',
         'credential_evidence_digest','nonce_digest','issued_at','expires_at',
         'nomos_decision_id','policy_fingerprint','provider_id',
         'provider_status','provider_updated_at','binding_id','binding_status',
         'binding_updated_at','nomos_input_hash','allowed','final_effect',
         'required_obligations_digest','completed_obligations_digest',
         'missing_obligations_digest'
       ]) AS required_key
       WHERE NOT (p_assertion ? required_key)
          OR p_assertion -> required_key = 'null'::jsonb
     ) THEN
    RAISE EXCEPTION 'external action verifier assertion shape is invalid'
      USING ERRCODE = '28000';
  END IF;
  IF p_assertion ->> 'schema' <> 'okg.external_action.verifier_assertion/v1'
     OR jsonb_typeof(p_assertion -> 'capability_generation') <> 'number'
     OR jsonb_typeof(p_assertion -> 'allowed') <> 'boolean'
     OR p_assertion ->> 'allowed' <> 'true'
     OR p_assertion ->> 'final_effect' <> 'allow'
     OR p_assertion_tag !~ '^sha256:[0-9a-f]{64}$'
     OR jsonb_typeof(p_expected) <> 'object'
     OR NOT (p_assertion @> p_expected) THEN
    RAISE EXCEPTION 'external action verifier assertion is invalid'
      USING ERRCODE = '28000';
  END IF;
  FOREACH digest_key IN ARRAY ARRAY[
    'transition_digest','credential_evidence_digest','nonce_digest',
    'policy_fingerprint','nomos_input_hash','required_obligations_digest',
    'completed_obligations_digest','missing_obligations_digest'
  ] LOOP
    IF p_assertion ->> digest_key !~ '^sha256:[0-9a-f]{64}$' THEN
      RAISE EXCEPTION 'external action verifier assertion digest is invalid'
        USING ERRCODE = '28000';
    END IF;
  END LOOP;
  IF p_assertion ->> 'issued_at'
       !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[.][0-9]{6}Z$'
     OR p_assertion ->> 'expires_at'
       !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[.][0-9]{6}Z$' THEN
    RAISE EXCEPTION 'external action verifier assertion time is invalid'
      USING ERRCODE = '28000';
  END IF;
  issued_value := (p_assertion ->> 'issued_at')::timestamptz;
  expires_value := (p_assertion ->> 'expires_at')::timestamptz;
  IF issued_value > observed_at + interval '5 seconds'
     OR issued_value < observed_at - interval '35 seconds'
     OR expires_value <= observed_at
     OR expires_value > issued_value + interval '30 seconds' THEN
    RAISE EXCEPTION 'external action verifier assertion is stale'
      USING ERRCODE = '28000';
  END IF;
  capability_status := okg.external_action_verifier_capability_status();
  IF p_assertion ->> 'deployment' IS DISTINCT FROM
       capability_status ->> 'deployment'
     OR (p_assertion ->> 'capability_generation')::bigint IS DISTINCT FROM
       (capability_status ->> 'capability_generation')::bigint THEN
    RAISE EXCEPTION 'external action verifier assertion generation is stale'
      USING ERRCODE = '28000';
  END IF;
  expected_tag := okg.external_action_hmac_sha256(
    decode(capability_value, 'hex'),
    convert_to('okg-external-action-verifier-assertion-v1', 'UTF8')
      || decode('00', 'hex')
      || convert_to(okg.jsonb_canonical_text(p_assertion), 'UTF8')
  );
  expected_tag_bytes := convert_to(expected_tag, 'UTF8');
  supplied_tag_bytes := convert_to(p_assertion_tag, 'UTF8');
  IF octet_length(expected_tag_bytes) <> octet_length(supplied_tag_bytes) THEN
    RAISE EXCEPTION 'external action verifier assertion authentication failed'
      USING ERRCODE = '28000';
  END IF;
  FOR tag_index IN 0..octet_length(expected_tag_bytes) - 1 LOOP
    tag_difference := tag_difference
      | (get_byte(expected_tag_bytes, tag_index)
         # get_byte(supplied_tag_bytes, tag_index));
  END LOOP;
  IF tag_difference <> 0 THEN
    RAISE EXCEPTION 'external action verifier assertion authentication failed'
      USING ERRCODE = '28000';
  END IF;
  RETURN p_assertion;
END
$external_action_verify_verifier_assertion$;
REVOKE ALL ON FUNCTION okg.external_action_verify_verifier_assertion(JSONB, TEXT, JSONB) FROM PUBLIC;

-- Provider idempotency keys use the same closed effect projection as their
-- audit digest, but retain the contract's unpadded base64url representation.
CREATE OR REPLACE FUNCTION okg.external_action_effect_key(p_value JSONB)
RETURNS TEXT
LANGUAGE SQL IMMUTABLE PARALLEL SAFE
SET search_path = pg_catalog, okg
AS $external_action_effect_key$
  SELECT replace(replace(rtrim(encode(
    sha256(
      convert_to('okg-external-action-effect-v1', 'UTF8')
      || decode('00', 'hex')
      || convert_to(okg.jsonb_canonical_text(p_value), 'UTF8')
    ), 'base64'), '='), '+', '-'), '/', '_')
$external_action_effect_key$;
REVOKE ALL ON FUNCTION okg.external_action_effect_key(JSONB) FROM PUBLIC;

CREATE OR REPLACE FUNCTION okg.external_action_nomos_authority_snapshot(
  p_deployment TEXT,
  p_authentication_audit_id TEXT,
  p_nomos_decision_id TEXT,
  p_expected_action TEXT,
  p_expected_transition_digest TEXT
) RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $external_action_nomos_authority_snapshot$
DECLARE
  bound_deployment TEXT := okg.external_action_session_deployment();
  audit_row okg.external_action_auth_audits%ROWTYPE;
  decision_row okg.nomos_decisions%ROWTYPE;
  provider_row okg.nomos_identity_providers%ROWTYPE;
  binding_row okg.nomos_subject_bindings%ROWTYPE;
  login_binding_row okg.external_action_api_role_bindings%ROWTYPE;
  provider_generation_digest TEXT;
  binding_generation_digest TEXT;
  provider_thumbprint TEXT;
  binding_thumbprint TEXT;
  observed_at TIMESTAMPTZ := clock_timestamp();
BEGIN
  PERFORM okg.external_action_verifier_capability_status();
  IF p_deployment IS NULL OR p_deployment <> bound_deployment
     OR p_authentication_audit_id IS NULL
     OR p_nomos_decision_id IS NULL
     OR p_expected_transition_digest IS NULL
     OR p_expected_transition_digest !~ '^sha256:[0-9a-f]{64}$' THEN
    RAISE EXCEPTION 'external action authority is not deployment-bound'
      USING ERRCODE = '28000';
  END IF;
  PERFORM pg_advisory_xact_lock(
    hashtextextended('okg.external_action.deployment:' || bound_deployment, 0)
  );
  SELECT * INTO audit_row
   FROM okg.external_action_auth_audits
   WHERE deployment_name = bound_deployment
     AND authentication_audit_id = p_authentication_audit_id;
  SELECT * INTO decision_row
   FROM okg.nomos_decisions
   WHERE deployment_name = bound_deployment
     AND decision_id = p_nomos_decision_id;
  SELECT * INTO login_binding_row
    FROM okg.external_action_api_role_bindings
   WHERE "session_user" = session_user
     AND deployment_name = bound_deployment
     AND status = 'active';
  IF audit_row.authentication_audit_id IS NULL
     OR decision_row.decision_id IS NULL
     OR decision_row.effect <> 'allow'
     OR decision_row.subject IS DISTINCT FROM audit_row.separation_subject
     OR (p_expected_action <> '' AND decision_row.action <> p_expected_action)
     OR audit_row.verifier_transition_digest IS DISTINCT FROM p_expected_transition_digest
     OR audit_row.verifier_assertion_expires_at <= observed_at
     OR audit_row.verifier_policy_input_digest IS DISTINCT FROM decision_row.input_hash
     OR audit_row.verifier_capability_generation IS DISTINCT FROM
          login_binding_row.verifier_capability_generation
     OR audit_row.verifier_required_obligations_digest IS DISTINCT FROM
          okg.external_action_hash(
            'okg-external-action-required-obligations-v1',
            COALESCE(decision_row.obligations, '[]'::jsonb)
          )
     OR audit_row.verifier_completed_obligations_digest IS DISTINCT FROM
          okg.external_action_hash(
            'okg-external-action-completed-obligations-v1',
            COALESCE(decision_row.obligations, '[]'::jsonb)
          )
     OR audit_row.verifier_missing_obligations_digest IS DISTINCT FROM
          okg.external_action_hash(
            'okg-external-action-missing-obligations-v1', '[]'::jsonb
          ) THEN
    RAISE EXCEPTION 'Nomos authority is unavailable or stale'
      USING ERRCODE = '28000';
  END IF;
  IF audit_row.authentication_method_code NOT IN ('jwt','jwt_dpop','mtls','trusted_proxy') THEN
    RAISE EXCEPTION 'authentication audit method is not supported by the external-action boundary'
      USING ERRCODE = '28000';
  END IF;
  SELECT * INTO provider_row
    FROM okg.nomos_identity_providers
   WHERE deployment_name = bound_deployment
     AND provider_id = audit_row.nomos_provider_id
     AND status = 'active'
     AND (effective_from IS NULL OR effective_from <= observed_at)
     AND (effective_until IS NULL OR observed_at < effective_until);
  SELECT * INTO binding_row
    FROM okg.nomos_subject_bindings
   WHERE deployment_name = bound_deployment
     AND binding_id = audit_row.nomos_subject_binding_id
     AND provider_id = audit_row.nomos_provider_id
     AND issuer = provider_row.issuer
     AND subject = audit_row.separation_subject
     AND status = 'active'
     AND (effective_from IS NULL OR effective_from <= observed_at)
     AND (effective_until IS NULL OR observed_at < effective_until);
  provider_generation_digest := COALESCE(
    provider_row.metadata ->> 'credential_generation_digest',
    provider_row.metadata ->> 'provider_credential_generation_digest',
    provider_row.metadata ->> 'credential_generation'
  );
  binding_generation_digest := COALESCE(
    binding_row.metadata ->> 'credential_generation_digest',
    binding_row.metadata ->> 'binding_credential_generation_digest',
    binding_row.metadata ->> 'credential_generation'
  );
  provider_thumbprint := COALESCE(
    provider_row.metadata ->> 'certificate_thumbprint',
    provider_row.metadata ->> 'sender_key_thumbprint'
  );
  binding_thumbprint := COALESCE(
    binding_row.metadata ->> 'certificate_thumbprint',
    binding_row.metadata ->> 'sender_key_thumbprint'
  );
  IF provider_row.provider_id IS NULL OR binding_row.binding_id IS NULL
     OR audit_row.nomos_decision_id IS DISTINCT FROM decision_row.decision_id
     OR (provider_generation_digest IS NOT NULL
         AND provider_generation_digest !~ '^sha256:[0-9a-f]{64}$')
     OR (binding_generation_digest IS NOT NULL
         AND binding_generation_digest !~ '^sha256:[0-9a-f]{64}$')
     OR (provider_thumbprint IS NOT NULL
         AND (btrim(provider_thumbprint) <> provider_thumbprint
              OR octet_length(provider_thumbprint) NOT BETWEEN 1 AND 512))
     OR (binding_thumbprint IS NOT NULL
         AND (btrim(binding_thumbprint) <> binding_thumbprint
              OR octet_length(binding_thumbprint) NOT BETWEEN 1 AND 512))
     OR audit_row.provider_status IS DISTINCT FROM provider_row.status
     OR audit_row.binding_status IS DISTINCT FROM binding_row.status
     OR audit_row.provider_updated_at IS DISTINCT FROM provider_row.updated_at
     OR audit_row.binding_updated_at IS DISTINCT FROM binding_row.updated_at
     OR (provider_generation_digest IS NOT NULL
         AND audit_row.provider_credential_generation_digest IS DISTINCT FROM provider_generation_digest)
     OR (binding_generation_digest IS NOT NULL
         AND audit_row.binding_credential_generation_digest IS DISTINCT FROM binding_generation_digest)
     OR (audit_row.credential_generation_digest IS NOT NULL
         AND ((provider_generation_digest IS NOT NULL
               AND audit_row.credential_generation_digest IS DISTINCT FROM provider_generation_digest)
              OR (binding_generation_digest IS NOT NULL
                  AND audit_row.credential_generation_digest IS DISTINCT FROM binding_generation_digest)))
     OR (audit_row.presented_credential_generation_digest IS NOT NULL
         AND ((provider_generation_digest IS NOT NULL
               AND audit_row.presented_credential_generation_digest IS DISTINCT FROM provider_generation_digest)
              OR (binding_generation_digest IS NOT NULL
                  AND audit_row.presented_credential_generation_digest IS DISTINCT FROM binding_generation_digest)))
     OR (audit_row.certificate_thumbprint IS NOT NULL
         AND provider_thumbprint IS NOT NULL
         AND audit_row.certificate_thumbprint IS DISTINCT FROM provider_thumbprint)
     OR (audit_row.certificate_thumbprint IS NOT NULL
         AND binding_thumbprint IS NOT NULL
         AND audit_row.certificate_thumbprint IS DISTINCT FROM binding_thumbprint)
     OR (audit_row.token_issued_at IS NOT NULL
         AND (audit_row.token_issued_at < provider_row.updated_at
              OR audit_row.token_issued_at < binding_row.updated_at))
     OR (audit_row.certificate_not_before IS NOT NULL
         AND (audit_row.certificate_not_before < provider_row.updated_at
              OR audit_row.certificate_not_before < binding_row.updated_at))
     OR audit_row.authentication_method_code IN ('jwt','jwt_dpop','mtls','trusted_proxy')
        AND (audit_row.provider_status IS NULL
             OR audit_row.binding_status IS NULL
             OR audit_row.provider_updated_at IS NULL
             OR audit_row.binding_updated_at IS NULL
             OR audit_row.provider_credential_generation_digest IS NULL
             OR audit_row.binding_credential_generation_digest IS NULL)
     OR audit_row.authentication_method_code IN ('jwt','jwt_dpop')
        AND audit_row.token_issued_at IS NULL
     OR audit_row.authentication_method_code = 'mtls'
        AND (audit_row.certificate_thumbprint IS NULL
             OR audit_row.certificate_not_before IS NULL
             OR audit_row.presented_credential_generation_digest IS NULL) THEN
    RAISE EXCEPTION 'Nomos provider, subject binding, or audit reference is stale'
      USING ERRCODE = '28000';
  END IF;
  RETURN jsonb_build_object(
    'deployment', bound_deployment,
    'authentication_audit_id', audit_row.authentication_audit_id,
    'nomos_decision_id', decision_row.decision_id,
    'nomos_provider_id', provider_row.provider_id,
    'nomos_subject_binding_id', binding_row.binding_id,
    'principal', audit_row.principal,
    'separation_subject', audit_row.separation_subject,
    'provider_status', provider_row.status,
    'provider_updated_at', provider_row.updated_at,
    'binding_status', binding_row.status,
    'binding_updated_at', binding_row.updated_at,
    'provider_credential_generation_digest', provider_generation_digest,
    'binding_credential_generation_digest', binding_generation_digest,
    'credential_generation_digest', audit_row.credential_generation_digest,
    'presented_credential_generation_digest', audit_row.presented_credential_generation_digest,
    'token_issued_at', audit_row.token_issued_at,
    'certificate_thumbprint', audit_row.certificate_thumbprint,
    'certificate_not_before', audit_row.certificate_not_before,
    'certificate_not_after', audit_row.certificate_not_after,
    'policy_digest', decision_row.policy_fingerprint,
    'action', decision_row.action,
    'effect', decision_row.effect,
    'reason_code', decision_row.reason_code
    ,'verifier_transition_digest', audit_row.verifier_transition_digest
    ,'verifier_capability_generation', audit_row.verifier_capability_generation
  );
END
$external_action_nomos_authority_snapshot$;

CREATE OR REPLACE FUNCTION okg.external_action_record_auth_audit(p_command JSONB)
RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $external_action_record_auth_audit$
DECLARE
  bound_deployment TEXT := okg.external_action_session_deployment();
  audit_row okg.external_action_auth_audits%ROWTYPE;
  provider_row okg.nomos_identity_providers%ROWTYPE;
  binding_row okg.nomos_subject_bindings%ROWTYPE;
  provider_generation_digest TEXT;
  binding_generation_digest TEXT;
  provider_thumbprint TEXT;
  binding_thumbprint TEXT;
  decision_row okg.nomos_decisions%ROWTYPE;
  credential_evidence_digest TEXT;
  verifier_assertion JSONB;
  digest_key TEXT;
  timestamp_key TEXT;
BEGIN
  PERFORM okg.external_action_require_command(
    p_command,
    ARRAY[
      'deployment','authentication_audit_id','request_id','principal',
      'separation_subject','nomos_provider_id','nomos_subject_binding_id',
      'authentication_method_code','issuer_or_trust_domain_digest',
      'sender_key_or_certificate_thumbprint','token_or_certificate_id_digest',
      'dpop_jti_digest','auth_config_digest','result_code','observed_at',
      'credential_expires_at','provider_status','binding_status',
      'provider_updated_at','binding_updated_at',
      'provider_credential_generation_digest','binding_credential_generation_digest',
      'credential_generation_digest','presented_credential_generation_digest',
      'token_issued_at','certificate_thumbprint','certificate_not_before',
      'certificate_not_after',
      'nomos_decision_id','verifier_assertion','verifier_assertion_tag'
    ],
    ARRAY[
      'deployment','authentication_audit_id','request_id','principal',
      'separation_subject','nomos_provider_id','nomos_subject_binding_id',
      'authentication_method_code','issuer_or_trust_domain_digest',
      'auth_config_digest','result_code'
      ,'nomos_decision_id','verifier_assertion','verifier_assertion_tag'
    ]
  );
  IF p_command ->> 'deployment' <> bound_deployment THEN
    RAISE EXCEPTION 'external action deployment is not bound to session_user'
      USING ERRCODE = '28000';
  END IF;
  IF p_command ->> 'authentication_method_code' NOT IN ('jwt','jwt_dpop','mtls','trusted_proxy') THEN
    RAISE EXCEPTION 'authentication audit method is not supported by the external-action boundary'
      USING ERRCODE = '22023';
  END IF;
  -- The submitted identity projection must describe one current database
  -- provider/binding pair.  This closes mixed-provider and stale-generation
  -- projections before they become durable audit evidence.
  SELECT * INTO provider_row
    FROM okg.nomos_identity_providers
   WHERE deployment_name = bound_deployment
     AND provider_id = p_command ->> 'nomos_provider_id';
  SELECT * INTO binding_row
    FROM okg.nomos_subject_bindings
   WHERE deployment_name = bound_deployment
     AND binding_id = p_command ->> 'nomos_subject_binding_id'
     AND provider_id = p_command ->> 'nomos_provider_id'
     AND issuer = provider_row.issuer
     AND subject = p_command ->> 'separation_subject';
  provider_generation_digest := COALESCE(
    provider_row.metadata ->> 'credential_generation_digest',
    provider_row.metadata ->> 'provider_credential_generation_digest',
    provider_row.metadata ->> 'credential_generation'
  );
  binding_generation_digest := COALESCE(
    binding_row.metadata ->> 'credential_generation_digest',
    binding_row.metadata ->> 'binding_credential_generation_digest',
    binding_row.metadata ->> 'credential_generation'
  );
  provider_thumbprint := COALESCE(
    provider_row.metadata ->> 'certificate_thumbprint',
    provider_row.metadata ->> 'sender_key_thumbprint'
  );
  binding_thumbprint := COALESCE(
    binding_row.metadata ->> 'certificate_thumbprint',
    binding_row.metadata ->> 'sender_key_thumbprint'
  );
  IF provider_row.provider_id IS NULL
     OR binding_row.binding_id IS NULL
     OR provider_row.status <> 'active'
     OR binding_row.status <> 'active'
     OR (provider_row.effective_from IS NOT NULL AND provider_row.effective_from > clock_timestamp())
     OR (provider_row.effective_until IS NOT NULL AND clock_timestamp() >= provider_row.effective_until)
     OR (binding_row.effective_from IS NOT NULL AND binding_row.effective_from > clock_timestamp())
     OR (binding_row.effective_until IS NOT NULL AND clock_timestamp() >= binding_row.effective_until)
     OR p_command ->> 'principal' IS DISTINCT FROM binding_row.subject
     OR p_command ->> 'separation_subject' IS DISTINCT FROM binding_row.subject
     OR p_command ->> 'issuer_or_trust_domain_digest' IS DISTINCT FROM
          'sha256:' || encode(
            sha256(convert_to(provider_row.issuer, 'UTF8')), 'hex')
     OR p_command ->> 'provider_status' IS DISTINCT FROM provider_row.status
     OR p_command ->> 'binding_status' IS DISTINCT FROM binding_row.status
     OR (provider_generation_digest IS NULL
         AND p_command ->> 'provider_credential_generation_digest' IS NOT NULL)
     OR (provider_generation_digest IS NOT NULL
         AND p_command ->> 'provider_credential_generation_digest' IS DISTINCT FROM provider_generation_digest)
     OR (binding_generation_digest IS NULL
         AND p_command ->> 'binding_credential_generation_digest' IS NOT NULL)
     OR (binding_generation_digest IS NOT NULL
         AND p_command ->> 'binding_credential_generation_digest' IS DISTINCT FROM binding_generation_digest)
     OR (provider_thumbprint IS NOT NULL
         AND p_command ->> 'certificate_thumbprint' IS NOT NULL
         AND p_command ->> 'certificate_thumbprint' IS DISTINCT FROM provider_thumbprint)
     OR (binding_thumbprint IS NOT NULL
         AND p_command ->> 'certificate_thumbprint' IS NOT NULL
         AND p_command ->> 'certificate_thumbprint' IS DISTINCT FROM binding_thumbprint)
     OR p_command ->> 'provider_updated_at' IS DISTINCT FROM to_char(provider_row.updated_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
     OR p_command ->> 'binding_updated_at' IS DISTINCT FROM to_char(binding_row.updated_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"') THEN
    RAISE EXCEPTION 'authentication audit identity is not a live database provider binding'
      USING ERRCODE = '28000';
  END IF;
  -- JSONB ->> coerces numbers and booleans to text.  Every retained evidence
  -- field therefore checks its JSON type before the cast below; otherwise a
  -- caller could smuggle a non-canonical timestamp or digest into an audit.
  FOREACH digest_key IN ARRAY ARRAY[
    'issuer_or_trust_domain_digest','token_or_certificate_id_digest',
    'dpop_jti_digest','auth_config_digest',
    'provider_credential_generation_digest','binding_credential_generation_digest',
    'credential_generation_digest','presented_credential_generation_digest'
  ] LOOP
    IF p_command ? digest_key AND p_command -> digest_key <> 'null'::jsonb THEN
      IF jsonb_typeof(p_command -> digest_key) IS DISTINCT FROM 'string'
         OR p_command ->> digest_key !~ '^sha256:[0-9a-f]{64}$' THEN
        RAISE EXCEPTION 'authentication audit digest is not canonical'
          USING ERRCODE = '22023';
      END IF;
    END IF;
  END LOOP;
  FOREACH timestamp_key IN ARRAY ARRAY[
    'observed_at','credential_expires_at','provider_updated_at',
    'binding_updated_at','token_issued_at','certificate_not_before',
    'certificate_not_after'
  ] LOOP
    IF p_command ? timestamp_key AND p_command -> timestamp_key <> 'null'::jsonb THEN
      IF jsonb_typeof(p_command -> timestamp_key) IS DISTINCT FROM 'string'
         OR p_command ->> timestamp_key !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[.][0-9]{6}Z$' THEN
        RAISE EXCEPTION 'authentication audit timestamp is not canonical'
          USING ERRCODE = '22023';
      END IF;
      -- Reject impossible calendar values (the regex only establishes the
      -- wire shape; PostgreSQL performs the calendar validation here).
      PERFORM (p_command ->> timestamp_key)::timestamptz;
    END IF;
  END LOOP;
  FOREACH timestamp_key IN ARRAY ARRAY['provider_status','binding_status'] LOOP
    IF p_command ? timestamp_key AND p_command -> timestamp_key <> 'null'::jsonb
       AND (jsonb_typeof(p_command -> timestamp_key) IS DISTINCT FROM 'string'
            OR btrim(p_command ->> timestamp_key) = '') THEN
      RAISE EXCEPTION 'authentication audit status is not canonical'
        USING ERRCODE = '22023';
    END IF;
  END LOOP;
  IF p_command ? 'provider_status' AND p_command ->> 'provider_status' IS NOT NULL
     AND p_command ->> 'provider_status' NOT IN ('active','disabled','rotating','revoked') THEN
    RAISE EXCEPTION 'authentication audit provider status is not canonical' USING ERRCODE = '22023';
  END IF;
  IF p_command ? 'binding_status' AND p_command ->> 'binding_status' IS NOT NULL
     AND p_command ->> 'binding_status' NOT IN ('active','disabled') THEN
    RAISE EXCEPTION 'authentication audit binding status is not canonical' USING ERRCODE = '22023';
  END IF;
  IF p_command ? 'certificate_thumbprint' AND p_command -> 'certificate_thumbprint' <> 'null'::jsonb
     AND (jsonb_typeof(p_command -> 'certificate_thumbprint') IS DISTINCT FROM 'string'
          OR btrim(p_command ->> 'certificate_thumbprint') <> p_command ->> 'certificate_thumbprint'
          OR octet_length(p_command ->> 'certificate_thumbprint') NOT BETWEEN 1 AND 512) THEN
    RAISE EXCEPTION 'authentication audit certificate thumbprint is not canonical' USING ERRCODE = '22023';
  END IF;
  IF p_command ->> 'certificate_not_before' IS NOT NULL
     AND p_command ->> 'certificate_not_after' IS NOT NULL
     AND (p_command ->> 'certificate_not_after')::timestamptz
         < (p_command ->> 'certificate_not_before')::timestamptz THEN
    RAISE EXCEPTION 'authentication audit certificate interval is not canonical' USING ERRCODE = '22023';
  END IF;
  IF p_command ? 'sender_key_or_certificate_thumbprint'
     AND p_command ? 'certificate_thumbprint'
     AND p_command -> 'sender_key_or_certificate_thumbprint' <> 'null'::jsonb
     AND p_command -> 'certificate_thumbprint' <> 'null'::jsonb
     AND p_command ->> 'sender_key_or_certificate_thumbprint' IS DISTINCT FROM p_command ->> 'certificate_thumbprint' THEN
    RAISE EXCEPTION 'authentication audit certificate thumbprint disagrees with sender constraint' USING ERRCODE = '22023';
  END IF;
  IF p_command ->> 'authentication_method_code' IN ('jwt','jwt_dpop','mtls','trusted_proxy')
     AND (p_command ->> 'provider_status' IS NULL
          OR p_command ->> 'binding_status' IS NULL
          OR p_command ->> 'provider_updated_at' IS NULL
          OR p_command ->> 'binding_updated_at' IS NULL
          OR p_command ->> 'provider_credential_generation_digest' IS NULL
          OR p_command ->> 'binding_credential_generation_digest' IS NULL) THEN
    RAISE EXCEPTION 'authentication audit identity barrier is incomplete' USING ERRCODE = '28000';
  END IF;
  IF p_command ->> 'authentication_method_code' IN ('jwt','jwt_dpop')
     AND p_command ->> 'token_issued_at' IS NULL THEN
    RAISE EXCEPTION 'authentication audit JWT issuance evidence is incomplete' USING ERRCODE = '28000';
  END IF;
  IF p_command ->> 'authentication_method_code' = 'mtls'
     AND (p_command ->> 'certificate_thumbprint' IS NULL
          OR p_command ->> 'certificate_not_before' IS NULL
          OR p_command ->> 'presented_credential_generation_digest' IS NULL) THEN
    RAISE EXCEPTION 'authentication audit certificate evidence is incomplete' USING ERRCODE = '28000';
  END IF;
  IF p_command ->> 'nomos_decision_id' IS NOT NULL THEN
    SELECT d.* INTO decision_row
      FROM okg.nomos_decisions d
     WHERE d.deployment_name = bound_deployment
       AND d.decision_id = p_command ->> 'nomos_decision_id';
    IF decision_row.decision_id IS NULL
       OR decision_row.subject IS DISTINCT FROM p_command ->> 'separation_subject' THEN
      RAISE EXCEPTION 'Nomos decision subject is not the audit separation subject'
        USING ERRCODE = '42501';
    END IF;
  END IF;
  credential_evidence_digest := okg.external_action_hash(
    'okg-external-action-credential-evidence-v1',
    jsonb_build_object(
      'principal', p_command -> 'principal',
      'separation_subject', p_command -> 'separation_subject',
      'nomos_provider_id', p_command -> 'nomos_provider_id',
      'nomos_subject_binding_id', p_command -> 'nomos_subject_binding_id',
      'authentication_method_code', p_command -> 'authentication_method_code',
      'issuer_or_trust_domain_digest', p_command -> 'issuer_or_trust_domain_digest',
      'sender_key_or_certificate_thumbprint', p_command -> 'sender_key_or_certificate_thumbprint',
      'token_or_certificate_id_digest', p_command -> 'token_or_certificate_id_digest',
      'dpop_jti_digest', p_command -> 'dpop_jti_digest',
      'auth_config_digest', p_command -> 'auth_config_digest',
      'result_code', p_command -> 'result_code',
      'credential_expires_at', p_command -> 'credential_expires_at',
      'provider_status', p_command -> 'provider_status',
      'binding_status', p_command -> 'binding_status',
      'provider_updated_at', p_command -> 'provider_updated_at',
      'binding_updated_at', p_command -> 'binding_updated_at',
      'provider_credential_generation_digest', p_command -> 'provider_credential_generation_digest',
      'binding_credential_generation_digest', p_command -> 'binding_credential_generation_digest',
      'credential_generation_digest', p_command -> 'credential_generation_digest',
      'presented_credential_generation_digest', p_command -> 'presented_credential_generation_digest',
      'token_issued_at', p_command -> 'token_issued_at',
      'certificate_thumbprint', p_command -> 'certificate_thumbprint',
      'certificate_not_before', p_command -> 'certificate_not_before',
      'certificate_not_after', p_command -> 'certificate_not_after'
    )
  );
  verifier_assertion := okg.external_action_verify_verifier_assertion(
    p_command -> 'verifier_assertion',
    p_command ->> 'verifier_assertion_tag',
    jsonb_build_object(
      'deployment', bound_deployment,
      'authentication_audit_id', p_command ->> 'authentication_audit_id',
      'credential_evidence_digest', credential_evidence_digest,
      'nomos_decision_id', decision_row.decision_id,
      'policy_fingerprint', decision_row.policy_fingerprint,
      'provider_id', provider_row.provider_id,
      'provider_status', provider_row.status,
      'provider_updated_at', p_command ->> 'provider_updated_at',
      'binding_id', binding_row.binding_id,
      'binding_status', binding_row.status,
      'binding_updated_at', p_command ->> 'binding_updated_at',
      'nomos_input_hash', decision_row.input_hash,
      'allowed', true,
      'final_effect', 'allow'
    )
  );
  PERFORM pg_advisory_xact_lock(
    hashtextextended('okg.external_action.deployment:' || bound_deployment, 0)
  );
  INSERT INTO okg.external_action_auth_audits (
    authentication_audit_id, deployment_name, request_id, principal,
    separation_subject, nomos_provider_id, nomos_subject_binding_id,
    authentication_method_code, issuer_or_trust_domain_digest,
    sender_key_or_certificate_thumbprint, token_or_certificate_id_digest,
    dpop_jti_digest, auth_config_digest, result_code, observed_at,
    credential_expires_at, provider_status, binding_status,
    provider_updated_at, binding_updated_at,
    provider_credential_generation_digest, binding_credential_generation_digest,
    credential_generation_digest, presented_credential_generation_digest,
    token_issued_at, certificate_thumbprint, certificate_not_before,
    certificate_not_after,
    nomos_decision_id,
    verifier_assertion_nonce_digest, verifier_assertion_expires_at,
    verifier_transition_digest, verifier_policy_input_digest,
    verifier_required_obligations_digest,
    verifier_completed_obligations_digest,
    verifier_missing_obligations_digest, verifier_assertion_tag,
    verifier_capability_generation, store_record
  ) VALUES (
    p_command ->> 'authentication_audit_id', bound_deployment,
    p_command ->> 'request_id', p_command ->> 'principal',
    p_command ->> 'separation_subject', p_command ->> 'nomos_provider_id',
    p_command ->> 'nomos_subject_binding_id',
    p_command ->> 'authentication_method_code',
    p_command ->> 'issuer_or_trust_domain_digest',
    p_command ->> 'sender_key_or_certificate_thumbprint',
    p_command ->> 'token_or_certificate_id_digest',
    p_command ->> 'dpop_jti_digest', p_command ->> 'auth_config_digest',
    p_command ->> 'result_code',
    COALESCE((p_command ->> 'observed_at')::timestamptz, clock_timestamp()),
    NULLIF(p_command ->> 'credential_expires_at', '')::timestamptz,
    p_command ->> 'provider_status', p_command ->> 'binding_status',
    NULLIF(p_command ->> 'provider_updated_at', '')::timestamptz,
    NULLIF(p_command ->> 'binding_updated_at', '')::timestamptz,
    p_command ->> 'provider_credential_generation_digest',
    p_command ->> 'binding_credential_generation_digest',
    p_command ->> 'credential_generation_digest',
    p_command ->> 'presented_credential_generation_digest',
    NULLIF(p_command ->> 'token_issued_at', '')::timestamptz,
    p_command ->> 'certificate_thumbprint',
    NULLIF(p_command ->> 'certificate_not_before', '')::timestamptz,
    NULLIF(p_command ->> 'certificate_not_after', '')::timestamptz,
    p_command ->> 'nomos_decision_id',
    verifier_assertion ->> 'nonce_digest',
    (verifier_assertion ->> 'expires_at')::timestamptz,
    verifier_assertion ->> 'transition_digest',
    verifier_assertion ->> 'nomos_input_hash',
    verifier_assertion ->> 'required_obligations_digest',
    verifier_assertion ->> 'completed_obligations_digest',
    verifier_assertion ->> 'missing_obligations_digest',
    p_command ->> 'verifier_assertion_tag',
    (verifier_assertion ->> 'capability_generation')::bigint,
    p_command
  )
  ON CONFLICT (deployment_name, authentication_audit_id) DO NOTHING;
  SELECT * INTO audit_row
    FROM okg.external_action_auth_audits
   WHERE deployment_name = bound_deployment
     AND authentication_audit_id = p_command ->> 'authentication_audit_id';
  IF audit_row.authentication_audit_id IS NULL THEN
    RAISE EXCEPTION 'authentication audit was not stored'
      USING ERRCODE = '40001';
  END IF;
  IF audit_row.request_id <> p_command ->> 'request_id'
     OR audit_row.principal <> p_command ->> 'principal'
     OR audit_row.separation_subject <> p_command ->> 'separation_subject'
     OR audit_row.auth_config_digest <> p_command ->> 'auth_config_digest'
     OR audit_row.nomos_provider_id <> p_command ->> 'nomos_provider_id'
     OR audit_row.nomos_subject_binding_id <> p_command ->> 'nomos_subject_binding_id'
     OR audit_row.authentication_method_code <> p_command ->> 'authentication_method_code'
     OR audit_row.issuer_or_trust_domain_digest <> p_command ->> 'issuer_or_trust_domain_digest'
     OR audit_row.sender_key_or_certificate_thumbprint IS DISTINCT FROM p_command ->> 'sender_key_or_certificate_thumbprint'
     OR audit_row.token_or_certificate_id_digest IS DISTINCT FROM p_command ->> 'token_or_certificate_id_digest'
     OR audit_row.dpop_jti_digest IS DISTINCT FROM p_command ->> 'dpop_jti_digest'
     OR audit_row.result_code <> p_command ->> 'result_code'
     OR audit_row.credential_expires_at IS DISTINCT FROM NULLIF(p_command ->> 'credential_expires_at', '')::timestamptz
     OR audit_row.provider_status IS DISTINCT FROM p_command ->> 'provider_status'
     OR audit_row.binding_status IS DISTINCT FROM p_command ->> 'binding_status'
     OR audit_row.provider_updated_at IS DISTINCT FROM NULLIF(p_command ->> 'provider_updated_at', '')::timestamptz
     OR audit_row.binding_updated_at IS DISTINCT FROM NULLIF(p_command ->> 'binding_updated_at', '')::timestamptz
     OR audit_row.provider_credential_generation_digest IS DISTINCT FROM p_command ->> 'provider_credential_generation_digest'
     OR audit_row.binding_credential_generation_digest IS DISTINCT FROM p_command ->> 'binding_credential_generation_digest'
     OR audit_row.credential_generation_digest IS DISTINCT FROM p_command ->> 'credential_generation_digest'
     OR audit_row.presented_credential_generation_digest IS DISTINCT FROM p_command ->> 'presented_credential_generation_digest'
     OR audit_row.token_issued_at IS DISTINCT FROM NULLIF(p_command ->> 'token_issued_at', '')::timestamptz
     OR audit_row.certificate_thumbprint IS DISTINCT FROM p_command ->> 'certificate_thumbprint'
     OR audit_row.certificate_not_before IS DISTINCT FROM NULLIF(p_command ->> 'certificate_not_before', '')::timestamptz
     OR audit_row.certificate_not_after IS DISTINCT FROM NULLIF(p_command ->> 'certificate_not_after', '')::timestamptz
     OR audit_row.nomos_decision_id IS DISTINCT FROM p_command ->> 'nomos_decision_id'
     OR audit_row.verifier_assertion_nonce_digest IS DISTINCT FROM verifier_assertion ->> 'nonce_digest'
     OR audit_row.verifier_assertion_tag IS DISTINCT FROM p_command ->> 'verifier_assertion_tag'
     OR audit_row.store_record IS DISTINCT FROM p_command THEN
    RAISE EXCEPTION 'authentication audit id was reused with a different request'
      USING ERRCODE = '40001';
  END IF;
  RETURN jsonb_build_object(
    'replay', FALSE,
    'authentication_audit_id', audit_row.authentication_audit_id,
    'deployment', audit_row.deployment_name,
    'request_id', audit_row.request_id,
    'principal', audit_row.principal,
    'separation_subject', audit_row.separation_subject,
    'nomos_provider_id', audit_row.nomos_provider_id,
    'nomos_subject_binding_id', audit_row.nomos_subject_binding_id,
    'authentication_method_code', audit_row.authentication_method_code,
    'issuer_or_trust_domain_digest', audit_row.issuer_or_trust_domain_digest,
    'sender_key_or_certificate_thumbprint', audit_row.sender_key_or_certificate_thumbprint,
    'token_or_certificate_id_digest', audit_row.token_or_certificate_id_digest,
    'dpop_jti_digest', audit_row.dpop_jti_digest,
    'auth_config_digest', audit_row.auth_config_digest,
    'result_code', audit_row.result_code,
    'observed_at', audit_row.observed_at,
    'credential_expires_at', audit_row.credential_expires_at,
    'provider_status', audit_row.provider_status,
    'binding_status', audit_row.binding_status,
    'provider_updated_at', audit_row.provider_updated_at,
    'binding_updated_at', audit_row.binding_updated_at,
    'provider_credential_generation_digest', audit_row.provider_credential_generation_digest,
    'binding_credential_generation_digest', audit_row.binding_credential_generation_digest,
    'credential_generation_digest', audit_row.credential_generation_digest,
    'presented_credential_generation_digest', audit_row.presented_credential_generation_digest,
    'token_issued_at', audit_row.token_issued_at,
    'certificate_thumbprint', audit_row.certificate_thumbprint,
    'certificate_not_before', audit_row.certificate_not_before,
    'certificate_not_after', audit_row.certificate_not_after,
    'nomos_decision_id', audit_row.nomos_decision_id
  );
END
$external_action_record_auth_audit$;

CREATE OR REPLACE FUNCTION okg.external_action_nomos_policy_bundle(
  p_scope TEXT,
  p_provider_id TEXT,
  p_binding_id TEXT
) RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $external_action_nomos_policy_bundle$
DECLARE
  bound_deployment TEXT := okg.external_action_session_deployment();
  observed_at TIMESTAMPTZ := clock_timestamp();
  activation_count INTEGER;
  activation_row okg.nomos_policy_activations%ROWTYPE;
  bundle_row okg.nomos_policy_bundles%ROWTYPE;
  provider_row okg.nomos_identity_providers%ROWTYPE;
  binding_row okg.nomos_subject_bindings%ROWTYPE;
  provider_generation_digest TEXT;
  binding_generation_digest TEXT;
BEGIN
  PERFORM okg.external_action_verifier_capability_status();
  IF p_scope IS NULL OR btrim(p_scope) = ''
     OR p_provider_id IS NULL OR btrim(p_provider_id) = ''
     OR p_binding_id IS NULL OR btrim(p_binding_id) = '' THEN
    RAISE EXCEPTION 'Nomos policy authority request is incomplete'
      USING ERRCODE = '22023';
  END IF;
  PERFORM pg_advisory_xact_lock(
    hashtextextended(
      'okg.external_action.deployment:' || bound_deployment, 0
    )
  );
  SELECT count(*)::INTEGER INTO activation_count
    FROM okg.nomos_policy_activations a
    JOIN okg.nomos_policy_bundles b
      ON b.deployment_name = a.deployment_name
     AND b.policy_fingerprint = a.policy_fingerprint
   WHERE a.deployment_name = bound_deployment
     AND a.scope = p_scope
     AND a.status = 'active'
     AND b.status = 'active'
     AND (b.effective_from IS NULL OR b.effective_from <= observed_at)
     AND (b.effective_until IS NULL OR observed_at < b.effective_until);
  IF activation_count <> 1 THEN
    RAISE EXCEPTION 'active Nomos policy bundle is unavailable'
      USING ERRCODE = '42501';
  END IF;
  SELECT a.* INTO activation_row
    FROM okg.nomos_policy_activations a
   WHERE a.deployment_name = bound_deployment
     AND a.scope = p_scope
     AND a.status = 'active';
  SELECT b.* INTO bundle_row
    FROM okg.nomos_policy_bundles b
   WHERE b.deployment_name = bound_deployment
     AND b.policy_fingerprint = activation_row.policy_fingerprint
     AND b.status = 'active'
     AND (b.effective_from IS NULL OR b.effective_from <= observed_at)
     AND (b.effective_until IS NULL OR observed_at < b.effective_until);
  SELECT p.* INTO provider_row
    FROM okg.nomos_identity_providers p
   WHERE p.deployment_name = bound_deployment
     AND p.provider_id = p_provider_id;
  SELECT s.* INTO binding_row
    FROM okg.nomos_subject_bindings s
   WHERE s.deployment_name = bound_deployment
     AND s.binding_id = p_binding_id
     AND s.provider_id = p_provider_id
     AND s.issuer = provider_row.issuer
     AND s.scope = p_scope;
  IF bundle_row.policy_fingerprint IS NULL
     OR provider_row.provider_id IS NULL
     OR binding_row.binding_id IS NULL
     OR provider_row.status <> 'active'
     OR binding_row.status <> 'active'
     OR (provider_row.effective_from IS NOT NULL
         AND provider_row.effective_from > observed_at)
     OR (provider_row.effective_until IS NOT NULL
         AND observed_at >= provider_row.effective_until)
     OR (binding_row.effective_from IS NOT NULL
         AND binding_row.effective_from > observed_at)
     OR (binding_row.effective_until IS NOT NULL
         AND observed_at >= binding_row.effective_until) THEN
    RAISE EXCEPTION 'active Nomos provider or subject binding is unavailable'
      USING ERRCODE = '42501';
  END IF;
  provider_generation_digest := COALESCE(
    provider_row.metadata ->> 'credential_generation_digest',
    provider_row.metadata ->> 'provider_credential_generation_digest',
    provider_row.metadata ->> 'credential_generation'
  );
  binding_generation_digest := COALESCE(
    binding_row.metadata ->> 'credential_generation_digest',
    binding_row.metadata ->> 'binding_credential_generation_digest',
    binding_row.metadata ->> 'credential_generation'
  );
  RETURN jsonb_build_object(
    'deployment', bound_deployment,
    'scope', activation_row.scope,
    'activation_id', activation_row.activation_id,
    'policy_bundle_id', bundle_row.policy_bundle_id,
    'policy_version', bundle_row.policy_version,
    'policy_fingerprint', bundle_row.policy_fingerprint,
    'policy_bundle_updated_at', bundle_row.updated_at,
    'bundle', bundle_row.bundle,
    'provider_id', provider_row.provider_id,
    'provider_status', provider_row.status,
    'provider_updated_at', provider_row.updated_at,
    'provider_credential_generation_digest', provider_generation_digest,
    'provider_certificate_thumbprint', COALESCE(
      provider_row.metadata ->> 'certificate_thumbprint',
      provider_row.metadata ->> 'sender_key_thumbprint'
    ),
    'binding_id', binding_row.binding_id,
    'subject', binding_row.subject,
    'binding_status', binding_row.status,
    'binding_updated_at', binding_row.updated_at,
    'binding_credential_generation_digest', binding_generation_digest,
    'binding_certificate_thumbprint', COALESCE(
      binding_row.metadata ->> 'certificate_thumbprint',
      binding_row.metadata ->> 'sender_key_thumbprint'
    ),
    'authority_barrier', jsonb_build_object(
      'policy_fingerprint', bundle_row.policy_fingerprint,
      'policy_bundle_updated_at', bundle_row.updated_at,
      'provider_id', provider_row.provider_id,
      'provider_status', provider_row.status,
      'provider_updated_at', provider_row.updated_at,
      'provider_credential_generation_digest', provider_generation_digest,
      'binding_id', binding_row.binding_id,
      'binding_status', binding_row.status,
      'binding_updated_at', binding_row.updated_at,
      'binding_credential_generation_digest', binding_generation_digest
    )
  );
END
$external_action_nomos_policy_bundle$;

CREATE OR REPLACE FUNCTION okg.external_action_nomos_persist_decision(
  p_command JSONB
) RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $external_action_nomos_persist_decision$
DECLARE
  bound_deployment TEXT := okg.external_action_session_deployment();
  decision_row okg.nomos_decisions%ROWTYPE;
  authority_row JSONB;
  inserted BOOLEAN;
  inserted_count INTEGER;
  rule JSONB;
  selected_rule JSONB;
  facts JSONB;
  expected JSONB;
  actual JSONB;
  match_key TEXT;
  rule_matches BOOLEAN;
  expected_effect TEXT;
  expected_reason_code TEXT;
  expected_matched_rule_ids JSONB;
  expected_obligations JSONB;
  expected_input_hash TEXT;
  expected_required_obligations_digest TEXT;
  expected_completed_obligations_digest TEXT;
  expected_missing_obligations_digest TEXT;
BEGIN
  PERFORM okg.external_action_require_command(
    p_command,
    ARRAY[
      'deployment','decision_id','policy_bundle_id','policy_version',
      'policy_fingerprint','input_hash','subject','action','resource',
      'classification','context','effect','reason_code','matched_rule_ids',
      'obligations','enforcement_point','target','created_at',
      'policy_scope','provider_id','binding_id','provider_status',
      'provider_updated_at','binding_status','binding_updated_at',
      'verifier_assertion','verifier_assertion_tag','allowed','final_effect',
      'required_obligations','completed_obligations','missing_obligations'
    ],
    ARRAY[
      'deployment','decision_id','input_hash','subject','action',
      'classification','effect','reason_code','enforcement_point',
      'policy_bundle_id','policy_version','policy_fingerprint',
      'policy_scope','provider_id','binding_id','provider_status',
      'provider_updated_at','binding_status','binding_updated_at',
      'verifier_assertion','verifier_assertion_tag','allowed','final_effect',
      'required_obligations','completed_obligations','missing_obligations'
    ]
  );
  IF p_command ->> 'deployment' <> bound_deployment THEN
    RAISE EXCEPTION 'external action deployment is not bound to session_user'
      USING ERRCODE = '28000';
  END IF;
  PERFORM pg_advisory_xact_lock(
    hashtextextended('okg.external_action.deployment:' || bound_deployment, 0)
  );
  authority_row := okg.external_action_nomos_policy_bundle(
    p_command ->> 'policy_scope',
    p_command ->> 'provider_id',
    p_command ->> 'binding_id'
  );
  IF authority_row ->> 'policy_bundle_id' IS DISTINCT FROM
       p_command ->> 'policy_bundle_id'
     OR authority_row ->> 'policy_version' IS DISTINCT FROM
       p_command ->> 'policy_version'
     OR authority_row ->> 'policy_fingerprint' IS DISTINCT FROM
       p_command ->> 'policy_fingerprint'
     OR authority_row ->> 'provider_status' IS DISTINCT FROM
       p_command ->> 'provider_status'
     OR (authority_row ->> 'provider_updated_at')::timestamptz IS DISTINCT FROM
       (p_command ->> 'provider_updated_at')::timestamptz
     OR authority_row ->> 'binding_status' IS DISTINCT FROM
       p_command ->> 'binding_status'
     OR (authority_row ->> 'binding_updated_at')::timestamptz IS DISTINCT FROM
       (p_command ->> 'binding_updated_at')::timestamptz THEN
    RAISE EXCEPTION 'Nomos policy or identity authority changed during decision'
      USING ERRCODE = '40001';
  END IF;
  -- The API supplies a policy decision projection, but that projection is
  -- not an authority.  Re-evaluate the request against the locked active
  -- bundle here, binding subject to the database-owned subject binding.  A
  -- caller can therefore not mint an allow by changing subject, effect, or
  -- reason fields in the JSON command.
  IF authority_row ->> 'subject' IS NULL
     OR jsonb_typeof(authority_row -> 'bundle') IS DISTINCT FROM 'object'
     OR jsonb_typeof(authority_row -> 'bundle' -> 'rules') IS DISTINCT FROM 'array'
     OR jsonb_typeof(p_command -> 'resource') IS DISTINCT FROM 'object'
     OR jsonb_typeof(p_command -> 'context') IS DISTINCT FROM 'object'
     OR jsonb_typeof(p_command -> 'matched_rule_ids') IS DISTINCT FROM 'array'
     OR jsonb_typeof(p_command -> 'obligations') IS DISTINCT FROM 'array' THEN
    RAISE EXCEPTION 'Nomos database authorization input is malformed'
      USING ERRCODE = '22023';
  END IF;
  -- Match Nomos core's fact construction exactly: canonical fields are
  -- seeded first, then resource and context fields overlay them.
  facts := jsonb_build_object(
      'subject', authority_row ->> 'subject',
      'action', p_command ->> 'action',
      'classification', p_command ->> 'classification'
    ) || COALESCE(p_command -> 'resource', '{}'::jsonb)
      || COALESCE(p_command -> 'context', '{}'::jsonb);
  FOR rule IN
    SELECT value
      FROM jsonb_array_elements(authority_row -> 'bundle' -> 'rules') AS entries(value)
     ORDER BY CASE
                WHEN value ->> 'priority' ~ '^-?[0-9]+$'
                  THEN (value ->> 'priority')::integer
                ELSE 0
              END DESC,
              value ->> 'rule_id'
  LOOP
    IF jsonb_typeof(rule) IS DISTINCT FROM 'object'
       OR jsonb_typeof(rule -> 'actions') IS DISTINCT FROM 'array'
       OR jsonb_typeof(rule -> 'subjects') IS DISTINCT FROM 'array'
       OR (rule ? 'match' AND jsonb_typeof(rule -> 'match') IS DISTINCT FROM 'object')
       OR rule ->> 'rule_id' IS NULL
       OR rule ->> 'effect' IS NULL THEN
      RAISE EXCEPTION 'Nomos active policy rule is malformed'
        USING ERRCODE = '42501';
    END IF;
    IF (rule -> 'actions') ? '*'
       OR (rule -> 'actions') ? (p_command ->> 'action') THEN
      IF (rule -> 'subjects') ? '*'
         OR (rule -> 'subjects') ? (authority_row ->> 'subject') THEN
        rule_matches := TRUE;
        FOR match_key, expected IN
          SELECT key, value
            FROM jsonb_each(COALESCE(rule -> 'match', '{}'::jsonb))
        LOOP
          IF NOT facts ? match_key THEN
            rule_matches := FALSE;
          ELSE
            actual := facts -> match_key;
            IF jsonb_typeof(expected) = 'array' THEN
              IF jsonb_typeof(actual) = 'array' THEN
                IF NOT (expected @> actual) THEN
                  rule_matches := FALSE;
                END IF;
              ELSIF NOT (expected @> jsonb_build_array(actual)) THEN
                rule_matches := FALSE;
              END IF;
            ELSIF jsonb_typeof(actual) = 'array' THEN
              IF NOT (actual @> jsonb_build_array(expected)) THEN
                rule_matches := FALSE;
              END IF;
            ELSIF actual IS DISTINCT FROM expected THEN
              rule_matches := FALSE;
            END IF;
          END IF;
          EXIT WHEN NOT rule_matches;
        END LOOP;
        IF rule_matches THEN
          selected_rule := rule;
          EXIT;
        END IF;
      END IF;
    END IF;
  END LOOP;
  IF selected_rule IS NULL THEN
    expected_effect := authority_row -> 'bundle' ->> 'default_effect';
    expected_reason_code := 'nomos.default_' || expected_effect;
    expected_matched_rule_ids := '[]'::jsonb;
    expected_obligations := '[]'::jsonb;
  ELSE
    expected_effect := selected_rule ->> 'effect';
    expected_reason_code := COALESCE(
      NULLIF(selected_rule ->> 'reason_code', ''),
      selected_rule ->> 'rule_id'
    );
    expected_matched_rule_ids := jsonb_build_array(selected_rule ->> 'rule_id');
    expected_obligations := COALESCE(selected_rule -> 'obligations', '[]'::jsonb);
  END IF;
  IF expected_effect IS NULL
     OR expected_effect NOT IN ('allow','deny','review','quarantine','redact','declassify')
     OR expected_reason_code IS NULL
     OR jsonb_typeof(expected_obligations) IS DISTINCT FROM 'array'
     OR p_command ->> 'subject' IS DISTINCT FROM authority_row ->> 'subject'
     OR p_command ->> 'effect' IS DISTINCT FROM expected_effect
     OR p_command ->> 'reason_code' IS DISTINCT FROM expected_reason_code
     OR COALESCE(p_command -> 'matched_rule_ids', '[]'::jsonb)
          IS DISTINCT FROM expected_matched_rule_ids
     OR COALESCE(p_command -> 'obligations', '[]'::jsonb)
          IS DISTINCT FROM expected_obligations THEN
    RAISE EXCEPTION 'Nomos decision is not the active database policy result'
      USING ERRCODE = '42501';
  END IF;
  expected_input_hash := 'sha256:' || encode(
    sha256(convert_to(okg.jsonb_canonical_text(jsonb_build_object(
      'subject', authority_row ->> 'subject',
      'action', p_command ->> 'action',
      'resource', COALESCE(p_command -> 'resource', '{}'::jsonb),
      'classification', p_command ->> 'classification',
      'context', COALESCE(p_command -> 'context', '{}'::jsonb)
    )), 'UTF8')), 'hex'
  );
  expected_required_obligations_digest := okg.external_action_hash(
    'okg-external-action-required-obligations-v1',
    COALESCE(p_command -> 'required_obligations', '[]'::jsonb)
  );
  expected_completed_obligations_digest := okg.external_action_hash(
    'okg-external-action-completed-obligations-v1',
    COALESCE(p_command -> 'completed_obligations', '[]'::jsonb)
  );
  expected_missing_obligations_digest := okg.external_action_hash(
    'okg-external-action-missing-obligations-v1',
    COALESCE(p_command -> 'missing_obligations', '[]'::jsonb)
  );
  IF p_command ->> 'input_hash' IS DISTINCT FROM expected_input_hash
     OR jsonb_typeof(p_command -> 'allowed') IS DISTINCT FROM 'boolean'
     OR p_command ->> 'allowed' IS DISTINCT FROM 'true'
     OR p_command ->> 'final_effect' IS DISTINCT FROM 'allow'
     OR jsonb_typeof(p_command -> 'required_obligations') IS DISTINCT FROM 'array'
     OR jsonb_typeof(p_command -> 'completed_obligations') IS DISTINCT FROM 'array'
     OR jsonb_typeof(p_command -> 'missing_obligations') IS DISTINCT FROM 'array'
     OR p_command -> 'required_obligations' IS DISTINCT FROM expected_obligations
     OR p_command -> 'completed_obligations' IS DISTINCT FROM expected_obligations
     OR p_command -> 'missing_obligations' IS DISTINCT FROM '[]'::jsonb THEN
    RAISE EXCEPTION 'Nomos strict runtime outcome is not an executable allow'
      USING ERRCODE = '42501';
  END IF;
  PERFORM okg.external_action_verify_verifier_assertion(
    p_command -> 'verifier_assertion',
    p_command ->> 'verifier_assertion_tag',
    jsonb_build_object(
      'deployment', bound_deployment,
      'operation', p_command ->> 'action',
      'nomos_decision_id', p_command ->> 'decision_id',
      'policy_fingerprint', p_command ->> 'policy_fingerprint',
      'provider_id', p_command ->> 'provider_id',
      'provider_status', p_command ->> 'provider_status',
      'provider_updated_at', to_char(
        (p_command ->> 'provider_updated_at')::timestamptz AT TIME ZONE 'UTC',
        'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
      ),
      'binding_id', p_command ->> 'binding_id',
      'binding_status', p_command ->> 'binding_status',
      'binding_updated_at', to_char(
        (p_command ->> 'binding_updated_at')::timestamptz AT TIME ZONE 'UTC',
        'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
      ),
      'nomos_input_hash', expected_input_hash,
      'allowed', true,
      'final_effect', 'allow',
      'required_obligations_digest', expected_required_obligations_digest,
      'completed_obligations_digest', expected_completed_obligations_digest,
      'missing_obligations_digest', expected_missing_obligations_digest
    )
  );
  INSERT INTO okg.nomos_decisions (
    deployment_name, decision_id, policy_bundle_id, policy_version,
    policy_fingerprint, input_hash, subject, action, resource,
    classification, context, effect, reason_code, matched_rule_ids,
    obligations, enforcement_point, target, created_at
  ) VALUES (
    bound_deployment, p_command ->> 'decision_id',
    p_command ->> 'policy_bundle_id', p_command ->> 'policy_version',
    p_command ->> 'policy_fingerprint', p_command ->> 'input_hash',
    p_command ->> 'subject', p_command ->> 'action',
    COALESCE(p_command -> 'resource', '{}'::jsonb),
    p_command ->> 'classification',
    COALESCE(p_command -> 'context', '{}'::jsonb),
    p_command ->> 'effect', p_command ->> 'reason_code',
    ARRAY(SELECT jsonb_array_elements_text(COALESCE(p_command -> 'matched_rule_ids', '[]'::jsonb))),
    COALESCE(p_command -> 'obligations', '[]'::jsonb),
    p_command ->> 'enforcement_point',
    COALESCE(p_command -> 'target', '{}'::jsonb),
    COALESCE((p_command ->> 'created_at')::timestamptz, clock_timestamp())
  ) ON CONFLICT (deployment_name, decision_id) DO NOTHING;
  GET DIAGNOSTICS inserted_count = ROW_COUNT;
  inserted := inserted_count = 1;
  SELECT * INTO decision_row
    FROM okg.nomos_decisions
   WHERE deployment_name = bound_deployment
     AND decision_id = p_command ->> 'decision_id';
  IF decision_row.decision_id IS NULL THEN
    RAISE EXCEPTION 'Nomos decision was not stored' USING ERRCODE = '40001';
  END IF;
  IF decision_row.policy_bundle_id IS DISTINCT FROM p_command ->> 'policy_bundle_id'
     OR decision_row.policy_version IS DISTINCT FROM p_command ->> 'policy_version'
     OR decision_row.policy_fingerprint IS DISTINCT FROM p_command ->> 'policy_fingerprint'
     OR decision_row.input_hash IS DISTINCT FROM p_command ->> 'input_hash'
     OR decision_row.subject IS DISTINCT FROM p_command ->> 'subject'
     OR decision_row.action IS DISTINCT FROM p_command ->> 'action'
     OR decision_row.resource IS DISTINCT FROM COALESCE(p_command -> 'resource', '{}'::jsonb)
     OR decision_row.classification IS DISTINCT FROM p_command ->> 'classification'
     OR decision_row.context IS DISTINCT FROM COALESCE(p_command -> 'context', '{}'::jsonb)
     OR decision_row.effect IS DISTINCT FROM p_command ->> 'effect'
     OR decision_row.reason_code IS DISTINCT FROM p_command ->> 'reason_code'
     OR decision_row.matched_rule_ids IS DISTINCT FROM ARRAY(
       SELECT jsonb_array_elements_text(
         COALESCE(p_command -> 'matched_rule_ids', '[]'::jsonb)
       )
     )
     OR decision_row.obligations IS DISTINCT FROM COALESCE(p_command -> 'obligations', '[]'::jsonb)
     OR decision_row.enforcement_point IS DISTINCT FROM p_command ->> 'enforcement_point'
     OR decision_row.target IS DISTINCT FROM COALESCE(p_command -> 'target', '{}'::jsonb)
     OR (p_command ->> 'created_at' IS NOT NULL
         AND decision_row.created_at IS DISTINCT FROM
           (p_command ->> 'created_at')::timestamptz) THEN
    RAISE EXCEPTION 'Nomos decision id was reused with a different request'
      USING ERRCODE = '40001';
  END IF;
  RETURN jsonb_build_object(
    'replay', NOT inserted,
    'deployment', decision_row.deployment_name,
    'decision_id', decision_row.decision_id,
    'input_hash', decision_row.input_hash,
    'subject', decision_row.subject,
    'action', decision_row.action,
    'effect', decision_row.effect,
    'reason_code', decision_row.reason_code,
    'created_at', decision_row.created_at
  );
END
$external_action_nomos_persist_decision$;

CREATE OR REPLACE FUNCTION okg.external_action_submit(p_command JSONB)
RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $external_action_submit$
DECLARE
  bound_deployment TEXT := okg.external_action_session_deployment();
  proposal_row okg.external_action_proposals%ROWTYPE;
  prior_proposal okg.external_action_proposals%ROWTYPE;
  current_lease_row okg.external_action_execution_epoch_leases%ROWTYPE;
  payload_hash TEXT;
  canonical_payload_hash TEXT;
  canonical_submission_content_hash TEXT;
  canonical_effect_instance_key TEXT;
  canonical_submission_request_hash TEXT;
  event_hash_value TEXT;
  event_projection JSONB;
  observed_at TIMESTAMPTZ := clock_timestamp();
BEGIN
  PERFORM okg.external_action_verifier_capability_status();
  PERFORM okg.external_action_require_command(
    p_command,
    ARRAY[
      'deployment','proposal_id','execution_epoch','effect_instance_key',
      'submission_content_hash','submission_request_hash',
      'submission_idempotency_key_hash','proposer_principal',
      'proposer_separation_subject','automation_id','automation_version',
      'run_id','correlation_id','action_type','target_ref','risk_tier',
      'action_schema_digest','action_contract_digest','executor_contract_digest',
      'payload_renderer_digest','governed_tool_binding_digest',
      'provider_connection_contract_digest','provider_capability_digest',
      'provider_operation_id','provider_operation_descriptor_digest',
      'original_payload_hash','requested_review_expires_at',
      'effective_review_expires_at','summary_code','evidence_digests',
      'review_state','execution_state','lifecycle_version','payload_version',
      'current_payload_hash','canonical_payload','parent_payload_hash',
      'difference_field_codes','authentication_audit_id','nomos_decision_id',
      'policy_digest','event_hash','created_at','updated_at',
      'action_contract_record'
    ],
    ARRAY[
      'deployment','proposal_id','execution_epoch','effect_instance_key',
      'submission_content_hash','submission_request_hash',
      'submission_idempotency_key_hash','proposer_principal',
      'proposer_separation_subject','automation_id','automation_version',
      'run_id','correlation_id','action_type','target_ref','risk_tier',
      'action_schema_digest','action_contract_digest','executor_contract_digest',
      'payload_renderer_digest','governed_tool_binding_digest',
      'provider_connection_contract_digest','provider_capability_digest',
      'provider_operation_id','provider_operation_descriptor_digest',
      'original_payload_hash','requested_review_expires_at',
      'effective_review_expires_at','summary_code','evidence_digests',
      'current_payload_hash','canonical_payload','authentication_audit_id',
      'nomos_decision_id','policy_digest','action_contract_record'
    ]
  );
  IF p_command ->> 'deployment' <> bound_deployment THEN
    RAISE EXCEPTION 'external action deployment is not bound to session_user'
      USING ERRCODE = '28000';
  END IF;
  PERFORM okg.external_action_nomos_authority_snapshot(
    bound_deployment, p_command ->> 'authentication_audit_id',
    p_command ->> 'nomos_decision_id', 'external_action.propose',
    p_command ->> 'submission_request_hash'
  );
  -- Every production submission retains the complete deployment-pinned
  -- contract.  Rebuild every semantic digest from that record and the
  -- canonical payload before resolving an idempotent replay.  The mapped API
  -- login is transport, never contract authority.
  IF jsonb_typeof(p_command->'action_contract_record') IS DISTINCT FROM 'object'
     OR NOT (p_command->'action_contract_record' ? 'outcome_reason_rules')
     OR jsonb_typeof(p_command->'action_contract_record'->'outcome_reason_rules') IS DISTINCT FROM 'object'
     OR p_command->'action_contract_record'->'outcome_reason_rules' = '{}'::jsonb
     OR EXISTS (
       SELECT 1
         FROM jsonb_object_keys(p_command->'action_contract_record') AS key_name
        WHERE key_name NOT IN (
          'action_type','action_schema_digest','executor_contract_digest',
          'payload_renderer_digest','governed_tool_binding_ref',
          'governed_tool_binding_digest','provider_connection_ref',
          'provider_connection_contract_digest','provider_capability_digest',
          'provider_operation_id','provider_operation_descriptor_digest',
          'provider_replay_mode','target_constraint','minimum_risk_tier',
          'review_and_execution_limits','diff_field_codes',
          'allowed_summary_codes','transition_reason_codes',
          'outcome_reason_rules'
        )
     )
     OR EXISTS (
       SELECT 1
         FROM unnest(ARRAY[
          'action_type','action_schema_digest','executor_contract_digest',
          'payload_renderer_digest','governed_tool_binding_ref',
          'governed_tool_binding_digest','provider_connection_ref',
          'provider_connection_contract_digest','provider_capability_digest',
          'provider_operation_id','provider_operation_descriptor_digest',
          'provider_replay_mode','target_constraint','minimum_risk_tier',
          'review_and_execution_limits','diff_field_codes',
          'allowed_summary_codes','transition_reason_codes',
          'outcome_reason_rules'
         ]) AS required_key
        WHERE NOT (p_command->'action_contract_record' ? required_key)
           OR p_command->'action_contract_record'->required_key = 'null'::jsonb
     ) THEN
    RAISE EXCEPTION 'action contract record is incomplete' USING ERRCODE='22023';
  END IF;
  IF p_command->>'action_type' IS DISTINCT FROM p_command->'action_contract_record'->>'action_type'
     OR p_command->>'action_schema_digest' IS DISTINCT FROM p_command->'action_contract_record'->>'action_schema_digest'
     OR p_command->>'executor_contract_digest' IS DISTINCT FROM p_command->'action_contract_record'->>'executor_contract_digest'
     OR p_command->>'payload_renderer_digest' IS DISTINCT FROM p_command->'action_contract_record'->>'payload_renderer_digest'
     OR p_command->>'governed_tool_binding_digest' IS DISTINCT FROM p_command->'action_contract_record'->>'governed_tool_binding_digest'
     OR p_command->>'provider_connection_contract_digest' IS DISTINCT FROM p_command->'action_contract_record'->>'provider_connection_contract_digest'
     OR p_command->>'provider_capability_digest' IS DISTINCT FROM p_command->'action_contract_record'->>'provider_capability_digest'
     OR p_command->>'provider_operation_id' IS DISTINCT FROM p_command->'action_contract_record'->>'provider_operation_id'
     OR p_command->>'provider_operation_descriptor_digest' IS DISTINCT FROM p_command->'action_contract_record'->>'provider_operation_descriptor_digest'
     OR (CASE p_command->>'risk_tier'
          WHEN 'low' THEN 0 WHEN 'moderate' THEN 1 WHEN 'high' THEN 2 WHEN 'critical' THEN 3 ELSE -1
        END) < (CASE p_command->'action_contract_record'->>'minimum_risk_tier'
          WHEN 'low' THEN 0 WHEN 'moderate' THEN 1 WHEN 'high' THEN 2 WHEN 'critical' THEN 3 ELSE 99
        END) THEN
    RAISE EXCEPTION 'submission fields do not match the retained action contract' USING ERRCODE='22023';
  END IF;
  BEGIN
    IF p_command->>'action_contract_digest' IS DISTINCT FROM okg.external_action_hash(
      'okg-external-action-contract-v1', p_command->'action_contract_record') THEN
      RAISE EXCEPTION 'action contract digest is not canonical' USING ERRCODE='22023';
    END IF;
    canonical_payload_hash:=okg.external_action_hash(
      'okg-external-action-payload-v1',
      convert_from(okg.external_action_json_bytes(p_command->'canonical_payload'),'UTF8')::jsonb);
    IF p_command->>'original_payload_hash' IS DISTINCT FROM canonical_payload_hash
       OR p_command->>'current_payload_hash' IS DISTINCT FROM canonical_payload_hash THEN
      RAISE EXCEPTION 'submission payload hash is not canonical' USING ERRCODE='22023';
    END IF;
    canonical_submission_content_hash:=okg.external_action_hash(
      'okg-external-action-submission-content-v1',jsonb_build_object(
        'automation_id',p_command->>'automation_id','automation_version',p_command->>'automation_version',
        'run_id',p_command->>'run_id','correlation_id',p_command->>'correlation_id',
        'action_type',p_command->>'action_type','target_ref',p_command->>'target_ref',
        'risk_tier',p_command->>'risk_tier','action_schema_digest',p_command->>'action_schema_digest',
        'action_contract_digest',p_command->>'action_contract_digest',
        'original_payload_hash',canonical_payload_hash,
        'requested_review_expires_at',to_char((p_command->>'requested_review_expires_at')::timestamptz AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
        'summary_code',p_command->>'summary_code','evidence_digests',p_command->'evidence_digests'));
    canonical_effect_instance_key:=okg.external_action_hash(
      'okg-external-action-effect-instance-v1',jsonb_build_object(
        'deployment',bound_deployment,
        'proposer_separation_subject',p_command->>'proposer_separation_subject',
        'submission_idempotency_key_hash',p_command->>'submission_idempotency_key_hash'));
    canonical_submission_request_hash:=okg.external_action_hash(
      'okg-external-action-proposal-request-v1',jsonb_build_object(
        'deployment',bound_deployment,'execution_epoch',(p_command->>'execution_epoch')::bigint,
        'effect_instance_key',canonical_effect_instance_key,
        'submission_content_hash',canonical_submission_content_hash,
        'proposer_separation_subject',p_command->>'proposer_separation_subject',
        'automation_id',p_command->>'automation_id','automation_version',p_command->>'automation_version',
        'run_id',p_command->>'run_id','correlation_id',p_command->>'correlation_id',
        'action_type',p_command->>'action_type','target_ref',p_command->>'target_ref',
        'risk_tier',p_command->>'risk_tier','action_schema_digest',p_command->>'action_schema_digest',
        'action_contract_digest',p_command->>'action_contract_digest',
        'executor_contract_digest',p_command->>'executor_contract_digest',
        'payload_renderer_digest',p_command->>'payload_renderer_digest',
        'governed_tool_binding_digest',p_command->>'governed_tool_binding_digest',
        'provider_connection_contract_digest',p_command->>'provider_connection_contract_digest',
        'provider_capability_digest',p_command->>'provider_capability_digest',
        'provider_operation_id',p_command->>'provider_operation_id',
        'provider_operation_descriptor_digest',p_command->>'provider_operation_descriptor_digest',
        'original_payload_hash',canonical_payload_hash,
        'requested_review_expires_at',to_char((p_command->>'requested_review_expires_at')::timestamptz AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
        'summary_code',p_command->>'summary_code','evidence_digests',p_command->'evidence_digests'));
    IF p_command->>'effect_instance_key' IS DISTINCT FROM canonical_effect_instance_key
       OR p_command->>'submission_content_hash' IS DISTINCT FROM canonical_submission_content_hash
       OR p_command->>'submission_request_hash' IS DISTINCT FROM canonical_submission_request_hash THEN
      RAISE EXCEPTION 'submission semantic hash is not canonical' USING ERRCODE='22023';
    END IF;
  END;
  PERFORM pg_advisory_xact_lock(
    hashtextextended('okg.external_action.deployment:' || bound_deployment, 0)
  );
  SELECT * INTO prior_proposal
    FROM okg.external_action_proposals
   WHERE deployment_name = bound_deployment
     AND proposer_separation_subject = p_command ->> 'proposer_separation_subject'
     AND submission_idempotency_key_hash = p_command ->> 'submission_idempotency_key_hash'
   FOR UPDATE;
  IF prior_proposal.proposal_id IS NOT NULL THEN
    IF prior_proposal.submission_request_hash <> p_command ->> 'submission_request_hash' THEN
      RAISE EXCEPTION 'submission idempotency key was reused with a different request'
        USING ERRCODE = '40001';
    END IF;
    RETURN jsonb_build_object(
      'replay', TRUE, 'proposal_id', prior_proposal.proposal_id,
      'deployment', prior_proposal.deployment_name,
      'effect_instance_key', prior_proposal.effect_instance_key,
      'review_state', prior_proposal.review_state,
      'execution_state', prior_proposal.execution_state,
      'lifecycle_version', prior_proposal.lifecycle_version,
      'payload_version', prior_proposal.payload_version,
      'current_payload_hash', prior_proposal.current_payload_hash
    );
  END IF;
  -- A new submission is checked against one database-clock observation and
  -- the currently accepted, unexpired execution lease.  Keep this after the
  -- exact idempotent replay above: retries must resolve even after a deadline
  -- or lease boundary has passed.
  IF (p_command ->> 'requested_review_expires_at')::timestamptz <= observed_at
     OR (p_command ->> 'effective_review_expires_at')::timestamptz <= observed_at
     OR (p_command ->> 'effective_review_expires_at')::timestamptz
          > (p_command ->> 'requested_review_expires_at')::timestamptz THEN
    RAISE EXCEPTION 'external action review deadline interval is invalid'
      USING ERRCODE = '22023';
  END IF;
  SELECT * INTO current_lease_row
    FROM okg.external_action_execution_epoch_leases
   WHERE deployment_name = bound_deployment
   ORDER BY epoch DESC, lease_sequence DESC
   LIMIT 1;
  IF current_lease_row.lease_row_id IS NULL
     OR current_lease_row.epoch <> (p_command ->> 'execution_epoch')::bigint
     OR current_lease_row.issued_at > observed_at
     OR current_lease_row.expires_at <= observed_at THEN
    RAISE EXCEPTION 'external action execution epoch lease is not current'
      USING ERRCODE = '40001';
  END IF;
  payload_hash := p_command ->> 'current_payload_hash';
  INSERT INTO okg.external_action_proposals (
    proposal_id,deployment_name,execution_epoch,effect_instance_key,
    submission_content_hash,submission_request_hash,
    submission_idempotency_key_hash,proposer_separation_subject,
    automation_id,automation_version,run_id,correlation_id,action_type,
    target_ref,risk_tier,action_schema_digest,action_contract_digest,
    executor_contract_digest,payload_renderer_digest,
    governed_tool_binding_digest,provider_connection_contract_digest,
    provider_capability_digest,provider_operation_id,
    provider_operation_descriptor_digest,original_payload_hash,
    requested_review_expires_at,effective_review_expires_at,summary_code,
    evidence_digests,review_state,execution_state,lifecycle_version,
    payload_version,current_payload_hash,created_at,updated_at,
    store_record,action_contract_record,submission_idempotency_record
  ) VALUES (
    p_command ->> 'proposal_id',bound_deployment,(p_command ->> 'execution_epoch')::bigint,
    p_command ->> 'effect_instance_key',p_command ->> 'submission_content_hash',
    p_command ->> 'submission_request_hash',p_command ->> 'submission_idempotency_key_hash',
    p_command ->> 'proposer_separation_subject',p_command ->> 'automation_id',
    p_command ->> 'automation_version',p_command ->> 'run_id',p_command ->> 'correlation_id',
    p_command ->> 'action_type',p_command ->> 'target_ref',p_command ->> 'risk_tier',
    p_command ->> 'action_schema_digest',p_command ->> 'action_contract_digest',
    p_command ->> 'executor_contract_digest',p_command ->> 'payload_renderer_digest',
    p_command ->> 'governed_tool_binding_digest',p_command ->> 'provider_connection_contract_digest',
    p_command ->> 'provider_capability_digest',p_command ->> 'provider_operation_id',
    p_command ->> 'provider_operation_descriptor_digest',p_command ->> 'original_payload_hash',
    (p_command ->> 'requested_review_expires_at')::timestamptz,
    (p_command ->> 'effective_review_expires_at')::timestamptz,
    p_command ->> 'summary_code',p_command -> 'evidence_digests',
    COALESCE(p_command ->> 'review_state','pending'),
    COALESCE(p_command ->> 'execution_state','unavailable'),
    COALESCE((p_command ->> 'lifecycle_version')::bigint,1),
    COALESCE((p_command ->> 'payload_version')::bigint,1),
    payload_hash,observed_at,observed_at,p_command,p_command -> 'action_contract_record',
    jsonb_build_object('key', p_command ->> 'submission_idempotency_key_hash',
                       'value', p_command ->> 'submission_request_hash')
  ) RETURNING * INTO proposal_row;
  INSERT INTO okg.external_action_payload_versions (
    deployment_name,proposal_id,payload_version,payload_hash,parent_payload_hash,
    canonical_payload,difference_field_codes,created_at,store_record
  ) VALUES (
    bound_deployment,proposal_row.proposal_id,proposal_row.payload_version,
    payload_hash,p_command ->> 'parent_payload_hash',
    okg.external_action_json_bytes(p_command -> 'canonical_payload'),
    COALESCE(p_command -> 'difference_field_codes','[]'::jsonb),
    observed_at,p_command
  );
  event_projection := jsonb_build_object(
    'proposal_id', proposal_row.proposal_id,
    'execution_epoch', proposal_row.execution_epoch,
    'effect_instance_key', proposal_row.effect_instance_key,
    'sequence', 1,
    'lifecycle_version', proposal_row.lifecycle_version,
    'event_type', 'submitted',
    'actor_kind', 'authenticated_principal',
    'actor_principal', p_command ->> 'proposer_principal',
    'actor_separation_subject', p_command ->> 'proposer_separation_subject',
    'authentication_audit_ref', p_command ->> 'authentication_audit_id',
    'nomos_decision_ref', p_command ->> 'nomos_decision_id',
    'deadline_kind', NULL::TEXT,
    'policy_digest', p_command ->> 'policy_digest',
    'before_review_state', NULL::TEXT,
    'after_review_state', proposal_row.review_state,
    'before_execution_state', NULL::TEXT,
    'after_execution_state', proposal_row.execution_state,
    'payload_version', proposal_row.payload_version,
    'payload_hash', payload_hash,
    'decision_id', NULL::TEXT,
    'decision_binding_hash', NULL::TEXT,
    'execution_id', NULL::TEXT,
    'execution_binding_hash', NULL::TEXT,
    'transition_reason_code', NULL::TEXT,
    'outcome', NULL::TEXT,
    'outcome_reason_code', NULL::TEXT,
    'outcome_request_hash', NULL::TEXT,
    'restore_recovery_digest', NULL::TEXT,
    'effective_at', NULL::TEXT,
    'recorded_at', to_char(observed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
    'previous_event_hash', NULL::TEXT
  );
  event_hash_value := okg.external_action_hash('okg-external-action-event-v1', event_projection);
  IF p_command ->> 'event_hash' IS NOT NULL
     AND p_command ->> 'event_hash' IS DISTINCT FROM event_hash_value THEN
    RAISE EXCEPTION 'caller-supplied event hash disagrees with the closed event projection'
      USING ERRCODE = '22023';
  END IF;
  INSERT INTO okg.external_action_events (
    deployment_name,proposal_id,execution_epoch,effect_instance_key,sequence,
    lifecycle_version,event_type,actor_kind,actor_principal,
    actor_separation_subject,authentication_audit_ref,nomos_decision_ref,
    policy_digest,before_review_state,after_review_state,before_execution_state,
    after_execution_state,payload_version,payload_hash,effective_at,recorded_at,
    previous_event_hash,event_hash,store_record
  ) VALUES (
    bound_deployment,proposal_row.proposal_id,proposal_row.execution_epoch,
    proposal_row.effect_instance_key,1,proposal_row.lifecycle_version,
    'submitted','authenticated_principal',p_command ->> 'proposer_principal',
    p_command ->> 'proposer_separation_subject',
    p_command ->> 'authentication_audit_id',p_command ->> 'nomos_decision_id',
    p_command ->> 'policy_digest',NULL,proposal_row.review_state,NULL,
    proposal_row.execution_state,proposal_row.payload_version,payload_hash,
    NULL,observed_at,NULL,event_hash_value,
    jsonb_build_object('event_projection', event_projection)
  );
  RETURN jsonb_build_object(
    'replay',FALSE,'proposal_id',proposal_row.proposal_id,
    'deployment',proposal_row.deployment_name,'effect_instance_key',proposal_row.effect_instance_key,
    'review_state',proposal_row.review_state,'execution_state',proposal_row.execution_state,
    'lifecycle_version',proposal_row.lifecycle_version,'payload_version',proposal_row.payload_version,
    'current_payload_hash',proposal_row.current_payload_hash,'event_hash',event_hash_value
  );
END
$external_action_submit$;
CREATE OR REPLACE FUNCTION okg.external_action_logical_json_record(p_record JSONB)
RETURNS JSONB
LANGUAGE sql IMMUTABLE
SET search_path = pg_catalog, okg
AS $external_action_logical_json_record$
  SELECT CASE
    WHEN p_record ? 'deployment_name' THEN
      (p_record - 'deployment_name')
      || jsonb_build_object('deployment', p_record -> 'deployment_name')
    ELSE p_record
  END
$external_action_logical_json_record$;
REVOKE ALL ON FUNCTION okg.external_action_logical_json_record(JSONB) FROM PUBLIC;

CREATE OR REPLACE FUNCTION okg.external_action_store_snapshot()
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, okg AS $$
DECLARE bound_deployment TEXT := okg.external_action_session_deployment();
BEGIN
  RETURN jsonb_build_object(
    'execution_epoch', COALESCE((SELECT max(epoch) FROM okg.external_action_execution_epoch_leases WHERE deployment_name = bound_deployment), 0),
    'execution_epoch_lease_digest', (SELECT envelope_digest FROM okg.external_action_execution_epoch_leases WHERE deployment_name = bound_deployment ORDER BY epoch DESC, lease_sequence DESC LIMIT 1),
    'proposals', COALESCE((SELECT jsonb_agg(jsonb_build_object('key', p.proposal_id, 'value', okg.external_action_logical_json_record(to_jsonb(p)) || jsonb_build_object('proposer_principal', p.proposer_separation_subject)) ORDER BY p.proposal_id) FROM okg.external_action_proposals p WHERE p.deployment_name = bound_deployment), '[]'::jsonb),
    'payload_versions', COALESCE((SELECT jsonb_agg(jsonb_build_object('bucket', v.proposal_id, 'value', okg.external_action_logical_json_record(to_jsonb(v)) || jsonb_build_object('version', v.payload_version, 'difference', v.difference_field_codes, 'canonical_payload', jsonb_build_object('__okg_bytes_b64', encode(v.canonical_payload, 'base64'))) ) ORDER BY v.proposal_id, v.payload_version) FROM okg.external_action_payload_versions v WHERE v.deployment_name = bound_deployment), '[]'::jsonb),
    'decisions', COALESCE((SELECT jsonb_agg(jsonb_build_object('bucket', d.proposal_id, 'value', jsonb_build_object('decision_id', d.decision_id, 'proposal_id', d.proposal_id, 'verdict', CASE WHEN d.verdict = 'approved' THEN 'approve' ELSE 'reject' END, 'lifecycle_version', d.approved_lifecycle_version, 'payload_version', d.approved_payload_version, 'payload_hash', d.approved_payload_hash, 'approver_principal', d.approver_principal, 'approver_separation_subject', d.approver_separation_subject, 'policy_digest', d.policy_digest, 'execute_before', d.execute_before, 'reason_code', d.reason_code, 'decision_binding_hash', d.decision_binding_hash, 'created_at', d.created_at)) ORDER BY d.proposal_id, d.decision_row_id) FROM okg.external_action_decisions d WHERE d.deployment_name = bound_deployment), '[]'::jsonb),
    'events', COALESCE((SELECT jsonb_agg(jsonb_build_object('bucket', e.effect_instance_key, 'value', okg.external_action_logical_json_record(to_jsonb(e))) ORDER BY e.effect_instance_key, e.sequence) FROM okg.external_action_events e WHERE e.deployment_name = bound_deployment), '[]'::jsonb),
    'restore_fences', COALESCE((SELECT jsonb_agg(jsonb_build_object('key', f.deployment_name, 'value', okg.external_action_logical_json_record(to_jsonb(f))) ORDER BY f.deployment_name) FROM okg.external_action_restore_fences f WHERE f.deployment_name = bound_deployment), '[]'::jsonb)
  );
END
$$;

-- The old generic mirror writer is not a durability boundary.  Typed RPCs
-- above are the only accepted production write surface; remove the legacy
-- entry point when upgrading a database that still has it.
DROP FUNCTION IF EXISTS okg.external_action_store_append(TEXT, JSONB);

CREATE OR REPLACE FUNCTION okg.external_action_store_resolve_principal(p_issuer TEXT, p_subject TEXT)
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, okg AS $$
DECLARE bound_deployment TEXT := okg.external_action_session_deployment(); result JSONB;
BEGIN
  PERFORM okg.external_action_verifier_capability_status();
  SELECT jsonb_build_object('principal', b.subject, 'separation_subject', b.subject, 'provider_id', p.provider_id, 'subject_binding_id', b.binding_id, 'issuer', p.issuer)
    INTO result
    FROM okg.nomos_identity_providers p
    JOIN okg.nomos_subject_bindings b ON b.deployment_name = p.deployment_name AND b.provider_id = p.provider_id AND b.issuer = p.issuer
   WHERE p.deployment_name = bound_deployment AND p.issuer = p_issuer AND p.status = 'active' AND b.status = 'active' AND b.claim_path = p.subject_claim AND b.claim_value = p_subject AND (b.effective_from IS NULL OR b.effective_from <= clock_timestamp()) AND (b.effective_until IS NULL OR clock_timestamp() < b.effective_until)
   ORDER BY b.priority DESC, b.binding_id
   LIMIT 1;
  IF result IS NULL THEN
    RAISE EXCEPTION 'deployment-pinned principal mapping is unavailable' USING ERRCODE = '28000';
  END IF;
  RETURN result;
END
$$;

CREATE OR REPLACE FUNCTION okg.external_action_insert_dpop_replay_nonce(p_deployment TEXT, p_issuer_digest TEXT, p_sender_key_thumbprint TEXT, p_jti_digest TEXT, p_request_method TEXT, p_request_uri_digest TEXT, p_access_token_hash TEXT, p_observed_at TIMESTAMPTZ, p_expires_at TIMESTAMPTZ)
RETURNS BOOLEAN LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, okg AS $$
DECLARE bound_deployment TEXT := okg.external_action_session_deployment(); inserted_count INTEGER;
BEGIN
  PERFORM okg.external_action_verifier_capability_status();
  IF p_deployment IS NULL OR p_deployment <> bound_deployment THEN
    RAISE EXCEPTION 'external action deployment is not bound to session_user' USING ERRCODE = '28000';
  END IF;
  INSERT INTO okg.external_action_dpop_replay_nonces (deployment_name, issuer_digest, sender_key_thumbprint, jti_digest, request_method, normalized_request_uri_digest, access_token_hash, observed_at, expires_at)
  VALUES (bound_deployment, p_issuer_digest, p_sender_key_thumbprint, p_jti_digest, p_request_method, p_request_uri_digest, p_access_token_hash, p_observed_at, p_expires_at)
  ON CONFLICT (deployment_name, issuer_digest, sender_key_thumbprint, jti_digest) DO NOTHING;
  GET DIAGNOSTICS inserted_count = ROW_COUNT;
  RETURN inserted_count = 1;
END
$$;

CREATE OR REPLACE FUNCTION okg.external_action_store_startup(p_state JSONB)
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, okg AS $$
DECLARE bound_deployment TEXT := okg.external_action_session_deployment(); requested TEXT;
BEGIN
  requested := p_state->>'deployment';
  IF requested IS NULL OR requested <> bound_deployment THEN
    RAISE EXCEPTION 'external action deployment is not bound to session_user' USING ERRCODE = '28000';
  END IF;
  RETURN jsonb_build_object('accepted', true, 'deployment', bound_deployment);
END
$$;

DO $external_action_runtime_function_grants$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'okg_external_action_api') THEN
    GRANT EXECUTE ON FUNCTION okg.external_action_store_snapshot() TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_nomos_policy_bundle(TEXT,TEXT,TEXT) TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_store_resolve_principal(TEXT, TEXT) TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_claim_owner_unavailability(TEXT) TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_insert_dpop_replay_nonce(TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TIMESTAMPTZ, TIMESTAMPTZ) TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_startup_login_binding() TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_store_startup(JSONB) TO okg_external_action_api;
  END IF;
  IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'okg_external_action_function_owner') THEN
    GRANT SELECT, INSERT, UPDATE ON
      okg.external_action_proposals,
      okg.external_action_payload_versions,
      okg.external_action_auth_audits,
      okg.external_action_dpop_replay_nonces,
      okg.external_action_decisions,
      okg.external_action_transition_idempotency,
      okg.external_action_executions,
      okg.external_action_outcomes,
      okg.external_action_events,
      okg.external_action_owner_unavailability_attestations,
      okg.external_action_execution_epoch_leases,
      okg.external_action_restore_attestations,
      okg.external_action_restore_fences,
      okg.external_action_restore_sessions,
      okg.external_action_restore_checkpoint_staging,
      okg.external_action_restore_completion_receipts,
      okg.external_action_restore_finalize_idempotency,
      okg.external_action_restore_checkpoint_entries,
      okg.external_action_restore_recoveries
    TO okg_external_action_function_owner;
    GRANT SELECT, UPDATE ON okg.external_action_api_role_bindings TO okg_external_action_function_owner;
    GRANT SELECT ON okg.nomos_policy_bundles, okg.nomos_policy_activations, okg.nomos_identity_providers, okg.nomos_subject_bindings, okg.nomos_decisions TO okg_external_action_function_owner;
    GRANT USAGE, SELECT ON
      okg.external_action_decisions_decision_row_id_seq,
      okg.external_action_dpop_replay_nonces_dpop_replay_nonce_id_seq,
      okg.external_action_events_event_id_seq,
      okg.external_action_execution_epoch_leases_lease_row_id_seq,
      okg.external_action_outcomes_outcome_id_seq,
      okg.external_action_owner_unavailability_attesta_attestation_id_seq,
      okg.external_action_payload_versions_payload_version_id_seq,
      okg.external_action_restore_attestations_attestation_row_id_seq,
      okg.external_action_restore_checkpoint__checkpoint_entry_row_id_seq,
      okg.external_action_restore_checkpoin_checkpoint_staging_row_id_seq,
      okg.external_action_restore_recoveries_recovery_row_id_seq,
      okg.external_action_restore_sessions_restore_session_row_id_seq,
      okg.external_action_restore_completio_completion_receipt_row_id_seq,
      okg.external_action_restore_finaliz_finalize_idempotency_row_id_seq,
      okg.external_action_transition_idempo_transition_idempotency_id_seq
    TO okg_external_action_function_owner;
    REVOKE ALL ON FUNCTION okg.external_action_session_deployment() FROM PUBLIC;
    REVOKE ALL ON FUNCTION okg.external_action_assert_deployment(TEXT) FROM PUBLIC;
    REVOKE ALL ON FUNCTION okg.external_action_store_snapshot() FROM PUBLIC;
    REVOKE ALL ON FUNCTION okg.external_action_nomos_policy_bundle(TEXT,TEXT,TEXT) FROM PUBLIC;
    REVOKE ALL ON FUNCTION okg.external_action_store_resolve_principal(TEXT, TEXT) FROM PUBLIC;
    REVOKE ALL ON FUNCTION okg.external_action_insert_dpop_replay_nonce(TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TIMESTAMPTZ, TIMESTAMPTZ) FROM PUBLIC;
    REVOKE ALL ON FUNCTION okg.external_action_startup_login_binding() FROM PUBLIC;
    REVOKE ALL ON FUNCTION okg.external_action_claim_owner_unavailability(TEXT) FROM PUBLIC;
    REVOKE ALL ON FUNCTION okg.external_action_store_startup(JSONB) FROM PUBLIC;
    ALTER FUNCTION okg.external_action_session_deployment() OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_assert_deployment(TEXT) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_store_snapshot() OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_nomos_policy_bundle(TEXT,TEXT,TEXT) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_nomos_authority_lock() OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_store_resolve_principal(TEXT, TEXT) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_insert_dpop_replay_nonce(TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TIMESTAMPTZ, TIMESTAMPTZ) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_startup_login_binding() OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_store_startup(JSONB) OWNER TO okg_external_action_function_owner;
  END IF;
END
$external_action_runtime_function_grants$;
CREATE OR REPLACE FUNCTION okg.external_action_review_transition(
  p_command JSONB
) RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $external_action_review_transition$
DECLARE
  bound_deployment TEXT := okg.external_action_session_deployment();
  proposal_row okg.external_action_proposals%ROWTYPE;
  previous_proposal okg.external_action_proposals%ROWTYPE;
  fence_row okg.external_action_restore_fences%ROWTYPE;
  replay_row okg.external_action_transition_idempotency%ROWTYPE;
  decision_row okg.external_action_decisions%ROWTYPE;
  operation_value TEXT;
  next_review_state TEXT;
  next_execution_state TEXT;
  next_lifecycle BIGINT;
  next_payload_version BIGINT;
  next_payload_hash TEXT;
  decision_id_value TEXT;
  decision_binding_value TEXT;
  deadline_value TIMESTAMPTZ;
  observed_at TIMESTAMPTZ := clock_timestamp();
  sequence_value BIGINT;
  previous_event_hash_value TEXT;
  event_hash_value TEXT;
  event_projection JSONB;
  response_body_value BYTEA;
  canonical_transition_request_hash TEXT;
  canonical_replacement_payload_hash TEXT;
BEGIN
  PERFORM okg.external_action_verifier_capability_status();
  PERFORM okg.external_action_require_command(
    p_command,
    ARRAY[
      'deployment','proposal_id','operation','actor_principal',
      'actor_separation_subject','idempotency_key_hash',
      'transition_request_hash','authentication_audit_id','nomos_decision_id',
      'policy_digest','expected_lifecycle_version','expected_payload_version',
      'expected_payload_hash','expected_decision_id',
      'expected_decision_binding_hash','replacement_payload',
      'canonical_payload','replacement_payload_hash','payload_version',
      'reason_code','execute_before','decision_id','decision_binding_hash',
      'event_hash','previous_event_hash','response_status',
      'response_media_type','response_body_jcs','result_event_ids',
      'result_payload_version','result_decision_id','result_lifecycle_version',
      'result_etag','action_contract_digest','action_contract_record'
    ],
    ARRAY[
      'deployment','proposal_id','operation','actor_principal',
      'actor_separation_subject','idempotency_key_hash',
      'transition_request_hash','authentication_audit_id','nomos_decision_id',
      'policy_digest'
    ]
  );
  IF p_command ->> 'deployment' <> bound_deployment THEN
    RAISE EXCEPTION 'external action deployment is not bound to session_user'
      USING ERRCODE = '28000';
  END IF;
  operation_value := p_command ->> 'operation';
  IF operation_value NOT IN ('edit','defer','approve','approve_with_edit','reject','cancel') THEN
    RAISE EXCEPTION 'external action transition operation is not allowed'
      USING ERRCODE = '22023';
  END IF;
  PERFORM pg_advisory_xact_lock(
    hashtextextended('okg.external_action.deployment:' || bound_deployment, 0)
  );
  SELECT * INTO proposal_row
    FROM okg.external_action_proposals
   WHERE deployment_name = bound_deployment
     AND proposal_id = p_command ->> 'proposal_id'
   FOR UPDATE;
  IF proposal_row.proposal_id IS NULL THEN
    RAISE EXCEPTION 'external action proposal was not found'
      USING ERRCODE = 'P0002';
  END IF;
  previous_proposal := proposal_row;
  -- Current Nomos authority is checked before resolving a transition retry;
  -- a stale exact request must not become an authorization oracle.
  PERFORM okg.external_action_nomos_authority_snapshot(
    bound_deployment,
    p_command ->> 'authentication_audit_id',
    p_command ->> 'nomos_decision_id',
    CASE WHEN operation_value = 'cancel'
         THEN 'external_action.cancel' ELSE 'external_action.decide' END,
    p_command ->> 'transition_request_hash'
  );
  IF proposal_row.action_contract_record ? 'outcome_reason_rules' THEN
    canonical_replacement_payload_hash:=NULL;
    IF p_command->>'replacement_payload_hash' IS NOT NULL THEN
      IF p_command->>'canonical_payload' IS NULL THEN
        RAISE EXCEPTION 'replacement payload bytes are required' USING ERRCODE='22023';
      END IF;
      canonical_replacement_payload_hash:=okg.external_action_hash(
        'okg-external-action-payload-v1',
        convert_from(okg.external_action_json_bytes(p_command->'canonical_payload'),'UTF8')::jsonb);
      IF p_command->>'replacement_payload_hash' IS DISTINCT FROM canonical_replacement_payload_hash THEN
        RAISE EXCEPTION 'replacement payload hash is not canonical' USING ERRCODE='22023';
      END IF;
    END IF;
  END IF;
  SELECT * INTO replay_row
    FROM okg.external_action_transition_idempotency
   WHERE deployment_name = bound_deployment
     AND proposal_id = proposal_row.proposal_id
     AND actor_separation_subject = p_command ->> 'actor_separation_subject'
     AND idempotency_key_hash = p_command ->> 'idempotency_key_hash'
   FOR UPDATE;
  IF replay_row.transition_idempotency_id IS NOT NULL THEN
    IF replay_row.transition_request_hash <> p_command ->> 'transition_request_hash' THEN
      RAISE EXCEPTION 'transition idempotency key was reused with a different request'
        USING ERRCODE = '40001';
    END IF;
    RETURN COALESCE(
      replay_row.store_record -> 'response_projection',
      jsonb_build_object(
        'replay', TRUE, 'proposal_id', proposal_row.proposal_id,
        'deployment', bound_deployment,
        'response_status', replay_row.response_status
      )
    ) || jsonb_build_object('replay', TRUE);
  END IF;
  -- A transition is a lifecycle mutation. Check the persisted startup fence
  -- after the authority/replay checks, while retaining the deployment
  -- advisory lock, so restore quarantine cannot be bypassed by updating an
  -- existing proposal instead of inserting a new row.
  SELECT * INTO fence_row
    FROM okg.external_action_restore_fences
   WHERE deployment_name = bound_deployment
   FOR SHARE;
  IF fence_row.deployment_name IS NOT NULL
     AND (fence_row.service_mode <> 'normal'
          OR fence_row.quarantine_state <> 'clear') THEN
    RAISE EXCEPTION 'external action lifecycle is quarantined for restore'
      USING ERRCODE = '40001';
  END IF;
  IF fence_row.deployment_name IS NOT NULL
     AND proposal_row.execution_epoch < fence_row.highest_accepted_epoch THEN
    RAISE EXCEPTION 'external action lifecycle epoch is stale'
      USING ERRCODE = '40001';
  END IF;
  IF operation_value <> 'cancel'
     AND p_command ->> 'actor_separation_subject'
         = proposal_row.proposer_separation_subject THEN
    RAISE EXCEPTION 'proposer and approver separation is required'
      USING ERRCODE = '42501';
  END IF;
  IF operation_value = 'cancel'
     AND p_command ->> 'actor_separation_subject'
         <> proposal_row.proposer_separation_subject THEN
    RAISE EXCEPTION 'only the proposer may cancel an external action'
      USING ERRCODE = '42501';
  END IF;
  IF p_command ? 'expected_lifecycle_version'
     AND (p_command ->> 'expected_lifecycle_version')::bigint
         <> proposal_row.lifecycle_version THEN
    RAISE EXCEPTION 'external action lifecycle precondition is stale'
      USING ERRCODE = '40001';
  END IF;
  IF p_command ? 'expected_payload_version'
     AND (p_command ->> 'expected_payload_version')::bigint
         <> proposal_row.payload_version THEN
    RAISE EXCEPTION 'external action payload precondition is stale'
      USING ERRCODE = '40001';
  END IF;
  IF p_command ? 'expected_payload_hash'
     AND p_command ->> 'expected_payload_hash'
         <> proposal_row.current_payload_hash THEN
    RAISE EXCEPTION 'external action payload binding is stale'
      USING ERRCODE = '40001';
  END IF;
  IF p_command ? 'expected_decision_id'
     AND p_command ->> 'expected_decision_id'
         IS DISTINCT FROM proposal_row.current_decision_id THEN
    RAISE EXCEPTION 'external action decision binding is stale'
      USING ERRCODE = '40001';
  END IF;
  IF p_command ? 'expected_decision_binding_hash'
     AND p_command ->> 'expected_decision_binding_hash'
         IS DISTINCT FROM proposal_row.current_decision_binding_hash THEN
    RAISE EXCEPTION 'external action decision binding is stale'
      USING ERRCODE = '40001';
  END IF;
  IF operation_value IN ('edit','defer','approve','approve_with_edit','reject')
     AND proposal_row.review_state NOT IN ('pending','edited') THEN
    RAISE EXCEPTION 'external action proposal is not reviewable'
      USING ERRCODE = '40001';
  END IF;
  IF operation_value = 'cancel'
     AND (proposal_row.review_state NOT IN ('pending','edited','approved')
          OR EXISTS (
            SELECT 1 FROM okg.external_action_executions e
             WHERE e.deployment_name = bound_deployment
               AND e.proposal_id = proposal_row.proposal_id
          )) THEN
    RAISE EXCEPTION 'external action proposal cannot be cancelled'
      USING ERRCODE = '40001';
  END IF;
  IF operation_value IN ('edit','defer','approve','approve_with_edit','reject')
     AND observed_at >= proposal_row.effective_review_expires_at THEN
    RAISE EXCEPTION 'external action review deadline has passed'
      USING ERRCODE = '22008';
  END IF;
  next_review_state := proposal_row.review_state;
  next_execution_state := proposal_row.execution_state;
  next_lifecycle := proposal_row.lifecycle_version + 1;
  next_payload_version := proposal_row.payload_version;
  next_payload_hash := proposal_row.current_payload_hash;
  IF operation_value IN ('edit','approve_with_edit') THEN
    IF NOT (p_command ? 'canonical_payload')
       OR NOT (p_command ? 'replacement_payload_hash') THEN
      RAISE EXCEPTION 'replacement payload and hash are required'
        USING ERRCODE = '22023';
    END IF;
    next_payload_version := proposal_row.payload_version + 1;
    next_payload_hash := p_command ->> 'replacement_payload_hash';
    INSERT INTO okg.external_action_payload_versions (
      deployment_name, proposal_id, payload_version, payload_hash,
      parent_payload_hash, canonical_payload, difference_field_codes,
      created_at, store_record
    ) VALUES (
      bound_deployment, proposal_row.proposal_id, next_payload_version,
      next_payload_hash, proposal_row.current_payload_hash,
      okg.external_action_json_bytes(p_command -> 'canonical_payload'),
      COALESCE(p_command -> 'result_event_ids', '[]'::jsonb),
      observed_at, p_command
    );
    next_review_state := 'edited';
  END IF;
  IF operation_value IN ('approve','approve_with_edit') THEN
    IF NOT (p_command ? 'decision_id')
       OR NOT (p_command ? 'execute_before') THEN
      RAISE EXCEPTION 'approval decision and deadline are required'
        USING ERRCODE = '22023';
    END IF;
    decision_id_value := p_command ->> 'decision_id';
    deadline_value := (p_command ->> 'execute_before')::timestamptz;
    IF deadline_value <= observed_at THEN
      RAISE EXCEPTION 'execution deadline has passed'
        USING ERRCODE = '22008';
    END IF;
    decision_binding_value := okg.external_action_hash(
      'okg-external-action-decision-binding-v1',
      jsonb_build_object(
        'deployment', bound_deployment,
        'execution_epoch', proposal_row.execution_epoch,
        'effect_instance_key', proposal_row.effect_instance_key,
        'proposal_id', proposal_row.proposal_id,
        'decision_id', decision_id_value,
        'approved_lifecycle_version', next_lifecycle,
        'approved_payload_version', next_payload_version,
        'approved_payload_hash', next_payload_hash,
        'action_type', proposal_row.action_type,
        'target_ref', proposal_row.target_ref,
        'action_schema_digest', proposal_row.action_schema_digest,
        'action_contract_digest', proposal_row.action_contract_digest,
        'executor_contract_digest', proposal_row.executor_contract_digest,
        'payload_renderer_digest', proposal_row.payload_renderer_digest,
        'governed_tool_binding_digest', proposal_row.governed_tool_binding_digest,
        'provider_connection_contract_digest', proposal_row.provider_connection_contract_digest,
        'provider_capability_digest', proposal_row.provider_capability_digest,
        'provider_operation_id', proposal_row.provider_operation_id,
        'provider_operation_descriptor_digest', proposal_row.provider_operation_descriptor_digest,
        'policy_digest', p_command ->> 'policy_digest',
        'execute_before', to_char(deadline_value AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
      )
    );
    IF p_command ->> 'decision_binding_hash' IS NOT NULL
       AND p_command ->> 'decision_binding_hash' IS DISTINCT FROM decision_binding_value THEN
      RAISE EXCEPTION 'caller-supplied decision binding hash disagrees with the closed decision binding'
        USING ERRCODE = '22023';
    END IF;
    INSERT INTO okg.external_action_decisions (
      decision_id,deployment_name,proposal_id,verdict,
      expected_lifecycle_version,expected_payload_version,
      approved_lifecycle_version,approved_payload_version,
      approved_payload_hash,action_type,target_ref,action_schema_digest,
      action_contract_digest,executor_contract_digest,payload_renderer_digest,
      governed_tool_binding_digest,provider_connection_contract_digest,
      provider_capability_digest,provider_operation_id,
      provider_operation_descriptor_digest,execution_epoch,effect_instance_key,
      approver_principal,approver_separation_subject,authentication_audit_id,
      nomos_decision_id,policy_digest,execute_before,reason_code,
      decision_binding_hash,request_hash,store_record,created_at
    ) VALUES (
      decision_id_value,bound_deployment,proposal_row.proposal_id,'approved',
      proposal_row.lifecycle_version,proposal_row.payload_version,
      next_lifecycle,next_payload_version,next_payload_hash,
      proposal_row.action_type,proposal_row.target_ref,proposal_row.action_schema_digest,
      proposal_row.action_contract_digest,proposal_row.executor_contract_digest,
      proposal_row.payload_renderer_digest,proposal_row.governed_tool_binding_digest,
      proposal_row.provider_connection_contract_digest,proposal_row.provider_capability_digest,
      proposal_row.provider_operation_id,proposal_row.provider_operation_descriptor_digest,
      proposal_row.execution_epoch,proposal_row.effect_instance_key,
      p_command ->> 'actor_principal',p_command ->> 'actor_separation_subject',
      p_command ->> 'authentication_audit_id',p_command ->> 'nomos_decision_id',
      p_command ->> 'policy_digest',deadline_value,p_command ->> 'reason_code',
      decision_binding_value,p_command ->> 'transition_request_hash',p_command,observed_at
    ) RETURNING * INTO decision_row;
    next_review_state := 'approved'; next_execution_state := 'ready';
  ELSIF operation_value = 'reject' THEN
    decision_id_value := p_command ->> 'decision_id';
    IF decision_id_value IS NULL THEN
      RAISE EXCEPTION 'rejection decision id is required' USING ERRCODE = '22023';
    END IF;
    decision_binding_value := okg.external_action_hash(
      'okg-external-action-decision-binding-v1',
      jsonb_build_object(
        'deployment', bound_deployment,
        'execution_epoch', proposal_row.execution_epoch,
        'effect_instance_key', proposal_row.effect_instance_key,
        'proposal_id', proposal_row.proposal_id,
        'decision_id', decision_id_value,
        'approved_lifecycle_version', next_lifecycle,
        'approved_payload_version', next_payload_version,
        'approved_payload_hash', next_payload_hash,
        'action_type', proposal_row.action_type,
        'target_ref', proposal_row.target_ref,
        'action_schema_digest', proposal_row.action_schema_digest,
        'action_contract_digest', proposal_row.action_contract_digest,
        'executor_contract_digest', proposal_row.executor_contract_digest,
        'payload_renderer_digest', proposal_row.payload_renderer_digest,
        'governed_tool_binding_digest', proposal_row.governed_tool_binding_digest,
        'provider_connection_contract_digest', proposal_row.provider_connection_contract_digest,
        'provider_capability_digest', proposal_row.provider_capability_digest,
        'provider_operation_id', proposal_row.provider_operation_id,
        'provider_operation_descriptor_digest', proposal_row.provider_operation_descriptor_digest,
        'policy_digest', p_command ->> 'policy_digest',
        'execute_before', NULL::TEXT
      )
    );
    IF p_command ->> 'decision_binding_hash' IS NOT NULL
       AND p_command ->> 'decision_binding_hash' IS DISTINCT FROM decision_binding_value THEN
      RAISE EXCEPTION 'caller-supplied decision binding hash disagrees with the closed decision binding'
        USING ERRCODE = '22023';
    END IF;
    INSERT INTO okg.external_action_decisions (
      decision_id,deployment_name,proposal_id,verdict,
      expected_lifecycle_version,expected_payload_version,
      approved_lifecycle_version,approved_payload_version,
      approved_payload_hash,action_type,target_ref,action_schema_digest,
      action_contract_digest,executor_contract_digest,payload_renderer_digest,
      governed_tool_binding_digest,provider_connection_contract_digest,
      provider_capability_digest,provider_operation_id,
      provider_operation_descriptor_digest,execution_epoch,effect_instance_key,
      approver_principal,approver_separation_subject,authentication_audit_id,
      nomos_decision_id,policy_digest,reason_code,decision_binding_hash,
      request_hash,store_record,created_at
    ) VALUES (
      decision_id_value,bound_deployment,proposal_row.proposal_id,'rejected',
      proposal_row.lifecycle_version,proposal_row.payload_version,
      next_lifecycle,next_payload_version,next_payload_hash,
      proposal_row.action_type,proposal_row.target_ref,proposal_row.action_schema_digest,
      proposal_row.action_contract_digest,proposal_row.executor_contract_digest,
      proposal_row.payload_renderer_digest,proposal_row.governed_tool_binding_digest,
      proposal_row.provider_connection_contract_digest,proposal_row.provider_capability_digest,
      proposal_row.provider_operation_id,proposal_row.provider_operation_descriptor_digest,
      proposal_row.execution_epoch,proposal_row.effect_instance_key,
      p_command ->> 'actor_principal',p_command ->> 'actor_separation_subject',
      p_command ->> 'authentication_audit_id',p_command ->> 'nomos_decision_id',
      p_command ->> 'policy_digest',p_command ->> 'reason_code',
      decision_binding_value,p_command ->> 'transition_request_hash',p_command,observed_at
    ) RETURNING * INTO decision_row;
    next_review_state := 'rejected'; next_execution_state := 'unavailable';
  ELSIF operation_value = 'cancel' THEN
    next_review_state := 'cancelled'; next_execution_state := 'unavailable';
  END IF;
  -- Validate the request hash only after the closed decision binding has been
  -- recomputed.  A caller-supplied decision binding therefore receives the
  -- typed 22023 refusal before a stale/forged request hash can mask it.
  IF proposal_row.action_contract_record ? 'outcome_reason_rules' THEN
    canonical_transition_request_hash:=okg.external_action_hash(
      'okg-external-action-transition-request-v1',jsonb_build_object(
        'operation',operation_value,
        'action_contract_digest',COALESCE(p_command->>'action_contract_digest',proposal_row.action_contract_digest),
        'expected_lifecycle_version',NULLIF(p_command->>'expected_lifecycle_version','')::bigint,
        'expected_payload_version',NULLIF(p_command->>'expected_payload_version','')::bigint,
        'expected_payload_hash',p_command->>'expected_payload_hash',
        'expected_decision_id',p_command->>'expected_decision_id',
        'expected_decision_binding_hash',p_command->>'expected_decision_binding_hash',
        'replacement_payload_hash',canonical_replacement_payload_hash,
        'execute_before',CASE WHEN p_command->>'execute_before' IS NULL THEN NULL ELSE to_char((p_command->>'execute_before')::timestamptz AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"') END,
        'reason_code',p_command->>'reason_code'));
    IF p_command->>'transition_request_hash' IS DISTINCT FROM canonical_transition_request_hash THEN
      RAISE EXCEPTION 'transition request hash is not canonical' USING ERRCODE='40001';
    END IF;
  END IF;
  UPDATE okg.external_action_proposals
     SET review_state = next_review_state, execution_state = next_execution_state,
         lifecycle_version = next_lifecycle, payload_version = next_payload_version,
         current_payload_hash = next_payload_hash,
         current_decision_id = CASE WHEN operation_value IN ('approve','approve_with_edit','reject') THEN decision_id_value ELSE current_decision_id END,
         current_decision_binding_hash = CASE WHEN operation_value IN ('approve','approve_with_edit','reject') THEN decision_binding_value ELSE current_decision_binding_hash END,
         execute_before = CASE WHEN operation_value IN ('approve','approve_with_edit') THEN deadline_value ELSE execute_before END,
         updated_at = observed_at
   WHERE deployment_name = bound_deployment AND proposal_id = proposal_row.proposal_id
  RETURNING * INTO proposal_row;
  SELECT COALESCE(max(sequence),0) + 1,
         (array_agg(event_hash ORDER BY sequence))[array_length(array_agg(event_hash ORDER BY sequence),1)]
    INTO sequence_value, previous_event_hash_value
    FROM okg.external_action_events
   WHERE deployment_name = bound_deployment AND effect_instance_key = proposal_row.effect_instance_key;
  IF p_command ->> 'previous_event_hash' IS NOT NULL AND p_command ->> 'previous_event_hash' IS DISTINCT FROM previous_event_hash_value THEN
    RAISE EXCEPTION 'external action event chain predecessor is stale' USING ERRCODE = '40001';
  END IF;
  event_projection := jsonb_build_object(
    'proposal_id', proposal_row.proposal_id,
    'execution_epoch', proposal_row.execution_epoch,
    'effect_instance_key', proposal_row.effect_instance_key,
    'sequence', sequence_value,
    'lifecycle_version', proposal_row.lifecycle_version,
    'event_type', CASE operation_value WHEN 'edit' THEN 'edited' WHEN 'defer' THEN 'deferred' WHEN 'approve' THEN 'approved' WHEN 'approve_with_edit' THEN 'approved' WHEN 'reject' THEN 'rejected' ELSE 'cancelled' END,
    'actor_kind', 'authenticated_principal',
    'actor_principal', p_command ->> 'actor_principal',
    'actor_separation_subject', p_command ->> 'actor_separation_subject',
    'authentication_audit_ref', p_command ->> 'authentication_audit_id',
    'nomos_decision_ref', p_command ->> 'nomos_decision_id',
    'deadline_kind', NULL::TEXT,
    'policy_digest', p_command ->> 'policy_digest',
    'before_review_state', previous_proposal.review_state,
    'after_review_state', proposal_row.review_state,
    'before_execution_state', previous_proposal.execution_state,
    'after_execution_state', proposal_row.execution_state,
    'payload_version', proposal_row.payload_version,
    'payload_hash', proposal_row.current_payload_hash,
    'decision_id', proposal_row.current_decision_id,
    'decision_binding_hash', proposal_row.current_decision_binding_hash,
    'execution_id', NULL::TEXT,
    'execution_binding_hash', NULL::TEXT,
    'transition_reason_code', p_command ->> 'reason_code',
    'outcome', NULL::TEXT,
    'outcome_reason_code', NULL::TEXT,
    'outcome_request_hash', NULL::TEXT,
    'restore_recovery_digest', NULL::TEXT,
    'effective_at', NULL::TEXT,
    'recorded_at', to_char(observed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
    'previous_event_hash', previous_event_hash_value
  );
  event_hash_value := okg.external_action_hash('okg-external-action-event-v1', event_projection);
  IF p_command ->> 'event_hash' IS NOT NULL
     AND p_command ->> 'event_hash' IS DISTINCT FROM event_hash_value THEN
    RAISE EXCEPTION 'caller-supplied event hash disagrees with the closed event projection'
      USING ERRCODE = '22023';
  END IF;
  INSERT INTO okg.external_action_events (
    deployment_name,proposal_id,execution_epoch,effect_instance_key,sequence,lifecycle_version,
    event_type,actor_kind,actor_principal,actor_separation_subject,
    authentication_audit_ref,nomos_decision_ref,policy_digest,
    before_review_state,after_review_state,before_execution_state,after_execution_state,
    payload_version,payload_hash,decision_id,decision_binding_hash,
    effective_at,recorded_at,previous_event_hash,event_hash,store_record
  ) VALUES (
    bound_deployment,proposal_row.proposal_id,proposal_row.execution_epoch,
    proposal_row.effect_instance_key,sequence_value,proposal_row.lifecycle_version,
    CASE operation_value WHEN 'edit' THEN 'edited' WHEN 'defer' THEN 'deferred'
      WHEN 'approve' THEN 'approved' WHEN 'approve_with_edit' THEN 'approved'
      WHEN 'reject' THEN 'rejected' ELSE 'cancelled' END,
    'authenticated_principal',p_command ->> 'actor_principal',
    p_command ->> 'actor_separation_subject',p_command ->> 'authentication_audit_id',
    p_command ->> 'nomos_decision_id',p_command ->> 'policy_digest',
    previous_proposal.review_state,proposal_row.review_state,
    previous_proposal.execution_state,proposal_row.execution_state,
    proposal_row.payload_version,proposal_row.current_payload_hash,
    proposal_row.current_decision_id,proposal_row.current_decision_binding_hash,
    NULL,observed_at,previous_event_hash_value,event_hash_value,
    jsonb_build_object('event_projection', event_projection)
  );
  response_body_value := COALESCE(okg.external_action_json_bytes(p_command -> 'response_body_jcs'), convert_to(jsonb_build_object('proposal_id',proposal_row.proposal_id,'deployment',bound_deployment,'review_state',proposal_row.review_state,'execution_state',proposal_row.execution_state,'lifecycle_version',proposal_row.lifecycle_version,'payload_version',proposal_row.payload_version,'current_payload_hash',proposal_row.current_payload_hash)::text,'utf8'));
  INSERT INTO okg.external_action_transition_idempotency (
    deployment_name,proposal_id,operation,actor_separation_subject,idempotency_key_hash,
    transition_request_hash,response_status,response_media_type,response_body_jcs,
    result_event_ids,result_payload_version,result_decision_id,result_lifecycle_version,
    result_etag,authentication_audit_id,nomos_decision_id,store_record,committed_at
  ) VALUES (
    bound_deployment,proposal_row.proposal_id,operation_value,p_command ->> 'actor_separation_subject',
    p_command ->> 'idempotency_key_hash',p_command ->> 'transition_request_hash',
    COALESCE((p_command ->> 'response_status')::integer,200),
    COALESCE(p_command ->> 'response_media_type','application/json'),response_body_value,
    COALESCE(p_command -> 'result_event_ids','[]'::jsonb),
    NULLIF(p_command ->> 'result_payload_version','')::bigint,
    NULLIF(p_command ->> 'result_decision_id',''),
    NULLIF(p_command ->> 'result_lifecycle_version','')::bigint,p_command ->> 'result_etag',
    p_command ->> 'authentication_audit_id',p_command ->> 'nomos_decision_id',
    jsonb_build_object('command',p_command,'response_projection',jsonb_build_object('replay',FALSE,'proposal_id',proposal_row.proposal_id,'deployment',bound_deployment,'review_state',proposal_row.review_state,'execution_state',proposal_row.execution_state,'lifecycle_version',proposal_row.lifecycle_version,'payload_version',proposal_row.payload_version,'current_payload_hash',proposal_row.current_payload_hash,'current_decision_id',proposal_row.current_decision_id,'current_decision_binding_hash',proposal_row.current_decision_binding_hash)),observed_at
  );
  RETURN jsonb_build_object('replay',FALSE,'proposal_id',proposal_row.proposal_id,'deployment',bound_deployment,'review_state',proposal_row.review_state,'execution_state',proposal_row.execution_state,'lifecycle_version',proposal_row.lifecycle_version,'payload_version',proposal_row.payload_version,'current_payload_hash',proposal_row.current_payload_hash,'current_decision_id',proposal_row.current_decision_id,'current_decision_binding_hash',proposal_row.current_decision_binding_hash,'event_hash',event_hash_value);
END
$external_action_review_transition$;

-- Prepare the claim handoff while holding the same deployment fence used by
-- the mutating claim RPC.  This is read-only: the service signs the returned
-- closed handoff fields, then calls external_action_claim in this transaction.
CREATE OR REPLACE FUNCTION okg.external_action_prepare_claim(p_command JSONB)
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $external_action_prepare_claim$
DECLARE
  bound_deployment TEXT := okg.external_action_session_deployment();
  proposal_row okg.external_action_proposals%ROWTYPE;
  decision_row okg.external_action_decisions%ROWTYPE;
  existing_execution okg.external_action_executions%ROWTYPE;
  payload_row okg.external_action_payload_versions%ROWTYPE;
  lease_digest_value TEXT;
  previous_event_hash_value TEXT;
  decision_event_hash_value TEXT;
  claim_event_hash_value TEXT;
  event_projection JSONB;
  observed_at TIMESTAMPTZ;
  event_sequence BIGINT;
  claim_lifecycle_version BIGINT;
  expiry_value JSONB;
BEGIN
  PERFORM okg.external_action_verifier_capability_status();
  PERFORM okg.external_action_require_command(
    p_command,
    ARRAY[
      'deployment','proposal_id','execution_id','decision_id',
      'worker_principal','worker_separation_subject',
      'approved_lifecycle_version','approved_payload_version',
      'approved_payload_hash','execution_epoch_lease_digest',
      'execution_binding_hash','request_hash',
      'authentication_audit_id',
      'nomos_decision_id','policy_digest','event_hash','previous_event_hash'
    ],
    ARRAY[
      'deployment','proposal_id','execution_id','decision_id',
      'worker_principal','worker_separation_subject',
      'approved_lifecycle_version','approved_payload_version',
      'approved_payload_hash','execution_epoch_lease_digest',
      'execution_binding_hash','request_hash',
      'authentication_audit_id',
      'nomos_decision_id','policy_digest'
    ]
  );
  IF p_command ->> 'deployment' <> bound_deployment THEN
    RAISE EXCEPTION 'external action deployment is not bound to session_user'
      USING ERRCODE='28000';
  END IF;
  PERFORM pg_advisory_xact_lock(
    hashtextextended('okg.external_action.deployment:' || bound_deployment,0)
  );
  PERFORM okg.external_action_nomos_authority_snapshot(
    bound_deployment,p_command ->> 'authentication_audit_id',
    p_command ->> 'nomos_decision_id','external_action.execute',
    p_command ->> 'request_hash'
  );
  SELECT * INTO proposal_row
    FROM okg.external_action_proposals
   WHERE deployment_name = bound_deployment
     AND proposal_id = p_command ->> 'proposal_id'
   FOR SHARE;
  IF proposal_row.proposal_id IS NULL THEN
    RAISE EXCEPTION 'external action proposal was not found' USING ERRCODE='P0002';
  END IF;
  SELECT * INTO existing_execution
    FROM okg.external_action_executions
   WHERE deployment_name = bound_deployment AND proposal_id = proposal_row.proposal_id
   FOR SHARE;
  IF existing_execution.execution_id IS NOT NULL THEN
    RAISE EXCEPTION 'external action proposal has already been claimed'
      USING ERRCODE='40001';
  END IF;
  -- An expired proposal is refused with a closed result, not an exception,
  -- so the caller can tell a passed deadline from an unavailable database.
  IF proposal_row.review_state = 'expired' THEN
    RETURN jsonb_build_object('refusal','proposal_expired',
      'proposal_id',proposal_row.proposal_id);
  END IF;
  IF proposal_row.review_state <> 'approved'
     OR proposal_row.execution_state <> 'ready' THEN
    RAISE EXCEPTION 'external action proposal is not ready to claim'
      USING ERRCODE='40001';
  END IF;
  observed_at := clock_timestamp();
  IF proposal_row.execute_before IS NULL OR observed_at >= proposal_row.execute_before THEN
    -- Record the elapsed execution deadline and RETURN the refusal: a raise
    -- here would roll back the approval_expired event with the transaction
    -- (issue #1925).  A NULL deadline cannot be materialized and stays the
    -- generic refusal.
    IF proposal_row.execute_before IS NOT NULL THEN
      expiry_value := okg.external_action_materialize_expiry(proposal_row.proposal_id);
      IF (expiry_value ->> 'expired')::boolean IS TRUE THEN
        RETURN jsonb_build_object('refusal','proposal_expired',
          'proposal_id',proposal_row.proposal_id);
      END IF;
    END IF;
    RAISE EXCEPTION 'external action execution deadline has passed' USING ERRCODE='22008';
  END IF;
  SELECT * INTO decision_row
    FROM okg.external_action_decisions
   WHERE deployment_name = bound_deployment
     AND decision_id = p_command ->> 'decision_id'
     AND proposal_id = proposal_row.proposal_id
   FOR SHARE;
  IF decision_row.decision_id IS NULL
     OR decision_row.verdict <> 'approved'
     OR decision_row.decision_binding_hash <> proposal_row.current_decision_binding_hash THEN
    RAISE EXCEPTION 'external action approval decision is unavailable' USING ERRCODE='40001';
  END IF;
  IF p_command->>'policy_digest' IS DISTINCT FROM decision_row.policy_digest THEN
    RAISE EXCEPTION 'external action policy binding is stale' USING ERRCODE='40001';
  END IF;
  IF (p_command ->> 'approved_lifecycle_version')::bigint <> proposal_row.lifecycle_version
     OR (p_command ->> 'approved_payload_version')::bigint <> proposal_row.payload_version
     OR p_command ->> 'approved_payload_hash' <> proposal_row.current_payload_hash THEN
    RAISE EXCEPTION 'external action approval payload binding is stale' USING ERRCODE='40001';
  END IF;
  SELECT envelope_digest INTO lease_digest_value
    FROM okg.external_action_execution_epoch_leases
   WHERE deployment_name = bound_deployment AND epoch = proposal_row.execution_epoch
   ORDER BY lease_sequence DESC LIMIT 1;
  IF lease_digest_value IS NULL
     OR lease_digest_value <> p_command ->> 'execution_epoch_lease_digest' THEN
    RAISE EXCEPTION 'external action execution epoch lease is stale' USING ERRCODE='40001';
  END IF;
  SELECT * INTO payload_row
    FROM okg.external_action_payload_versions
   WHERE deployment_name = bound_deployment
     AND proposal_id = proposal_row.proposal_id
     AND payload_version = proposal_row.payload_version
   FOR SHARE;
  IF payload_row.payload_version IS NULL THEN
    RAISE EXCEPTION 'external action payload is unavailable' USING ERRCODE='40001';
  END IF;
  SELECT COALESCE(max(sequence),0) + 1,
         (array_agg(event_hash ORDER BY sequence))[array_length(array_agg(event_hash ORDER BY sequence),1)]
    INTO event_sequence, previous_event_hash_value
    FROM okg.external_action_events
   WHERE deployment_name = bound_deployment
     AND effect_instance_key = proposal_row.effect_instance_key;
  SELECT event_hash INTO decision_event_hash_value
    FROM okg.external_action_events
   WHERE deployment_name = bound_deployment
     AND effect_instance_key = proposal_row.effect_instance_key
     AND event_type = 'approved'
     AND decision_id = decision_row.decision_id
   ORDER BY sequence DESC LIMIT 1;
  IF decision_event_hash_value IS NULL THEN
    RAISE EXCEPTION 'external action approval event is unavailable' USING ERRCODE='40001';
  END IF;
  claim_lifecycle_version := proposal_row.lifecycle_version + 1;
  event_projection := jsonb_build_object(
    'proposal_id', proposal_row.proposal_id,
    'execution_epoch', proposal_row.execution_epoch,
    'effect_instance_key', proposal_row.effect_instance_key,
    'sequence', event_sequence,
    'lifecycle_version', claim_lifecycle_version,
    'event_type', 'execution_claimed',
    'actor_kind', 'authenticated_principal',
    'actor_principal', p_command ->> 'worker_principal',
    'actor_separation_subject', p_command ->> 'worker_separation_subject',
    'authentication_audit_ref', p_command ->> 'authentication_audit_id',
    'nomos_decision_ref', p_command ->> 'nomos_decision_id',
    'deadline_kind', NULL::TEXT,
    'policy_digest', decision_row.policy_digest,
    'before_review_state', 'approved', 'after_review_state', 'approved',
    'before_execution_state', 'ready', 'after_execution_state', 'claimed',
    'payload_version', proposal_row.payload_version,
    'payload_hash', proposal_row.current_payload_hash,
    'decision_id', decision_row.decision_id,
    'decision_binding_hash', decision_row.decision_binding_hash,
    'execution_id', p_command ->> 'execution_id',
    'execution_binding_hash', p_command ->> 'execution_binding_hash',
    'transition_reason_code', NULL::TEXT, 'outcome', NULL::TEXT,
    'outcome_reason_code', NULL::TEXT, 'outcome_request_hash', NULL::TEXT,
    'restore_recovery_digest', NULL::TEXT, 'effective_at', NULL::TEXT,
    'recorded_at', to_char(observed_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
    'previous_event_hash', previous_event_hash_value
  );
  claim_event_hash_value := okg.external_action_hash(
    'okg-external-action-event-v1', event_projection
  );
  IF p_command ->> 'event_hash' IS NOT NULL
     AND p_command ->> 'event_hash' IS DISTINCT FROM claim_event_hash_value THEN
    RAISE EXCEPTION 'caller-supplied event hash disagrees with the prepared claim event'
      USING ERRCODE='22023';
  END IF;
  IF p_command ->> 'previous_event_hash' IS NOT NULL
     AND p_command ->> 'previous_event_hash' IS DISTINCT FROM previous_event_hash_value THEN
    RAISE EXCEPTION 'prepared claim event predecessor is stale' USING ERRCODE='40001';
  END IF;
  RETURN jsonb_build_object(
    'claimed_at', to_char(observed_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
    'previous_event_hash', previous_event_hash_value,
    'decision_event_hash', decision_event_hash_value,
    'claim_event_hash', claim_event_hash_value,
    'event_chain_head_hash', claim_event_hash_value,
    'claim_lifecycle_version', claim_lifecycle_version,
    'event_sequence', event_sequence
  );
END
$external_action_prepare_claim$;

CREATE OR REPLACE FUNCTION okg.external_action_claim(p_command JSONB)
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, okg AS $external_action_claim$
DECLARE
  bound_deployment TEXT := okg.external_action_session_deployment();
  proposal_row okg.external_action_proposals%ROWTYPE;
  execution_row okg.external_action_executions%ROWTYPE;
  existing_execution okg.external_action_executions%ROWTYPE;
  fence_row okg.external_action_restore_fences%ROWTYPE;
  decision_row okg.external_action_decisions%ROWTYPE;
  payload_row okg.external_action_payload_versions%ROWTYPE;
  existing_event okg.external_action_events%ROWTYPE;
  lease_digest_value TEXT;
  observed_at TIMESTAMPTZ := clock_timestamp();
  claimed_at_value TIMESTAMPTZ;
  decision_event_hash_value TEXT;
  claim_event_hash_value TEXT;
  prepared_claim_lifecycle_version BIGINT;
  prepared_event_sequence BIGINT;
  execution_binding_projection JSONB;
  provider_idempotency_projection JSONB;
  provider_idempotency_key_value TEXT;
  provider_idempotency_digest_value TEXT;
  event_sequence BIGINT;
  previous_event_hash_value TEXT;
  event_hash_value TEXT;
  event_projection JSONB;
  canonical_claim_request_hash TEXT;
  expiry_value JSONB;
BEGIN
  PERFORM okg.external_action_verifier_capability_status();
  PERFORM okg.external_action_require_command(
    p_command,
    ARRAY[
      'deployment','proposal_id','execution_id','decision_id',
      'worker_principal','worker_separation_subject','idempotency_key_hash',
      'request_hash','approved_lifecycle_version','approved_payload_version',
      'approved_payload_hash','execution_epoch_lease_digest',
      'supported_action_schema_digest','supported_action_contract_digest',
      'supported_executor_contract_digest','supported_payload_renderer_digest',
      'supported_governed_tool_binding_digest',
      'supported_provider_connection_contract_digest',
      'supported_provider_capability_digest','supported_provider_operation_id',
      'supported_provider_operation_descriptor_digest',
      'execution_binding_hash','provider_idempotency_key',
      'provider_idempotency_key_digest',
      'okg_claim_handoff_receipt_digest','handoff_protected_b64',
      'handoff_payload_b64','handoff_signature_b64','authentication_audit_id',
      'nomos_decision_id','policy_digest','event_hash','previous_event_hash',
      'claimed_at','decision_event_hash','claim_event_hash',
      'event_chain_head_hash','claim_lifecycle_version','event_sequence','if_match'
    ],
    ARRAY[
      'deployment','proposal_id','execution_id','decision_id',
      'worker_principal','worker_separation_subject','idempotency_key_hash',
      'request_hash','approved_lifecycle_version','approved_payload_version',
      'approved_payload_hash','execution_epoch_lease_digest',
      'supported_action_schema_digest','supported_action_contract_digest',
      'supported_executor_contract_digest','supported_payload_renderer_digest',
      'supported_governed_tool_binding_digest',
      'supported_provider_connection_contract_digest',
      'supported_provider_capability_digest','supported_provider_operation_id',
      'supported_provider_operation_descriptor_digest',
      'execution_binding_hash','provider_idempotency_key',
      'provider_idempotency_key_digest',
      'okg_claim_handoff_receipt_digest','handoff_protected_b64',
      'handoff_payload_b64','handoff_signature_b64','authentication_audit_id',
      'nomos_decision_id','event_hash','previous_event_hash','claimed_at',
      'decision_event_hash','claim_event_hash','event_chain_head_hash',
      'claim_lifecycle_version','event_sequence','if_match'
    ]
  );
  IF p_command ->> 'deployment' <> bound_deployment THEN RAISE EXCEPTION 'external action deployment is not bound to session_user' USING ERRCODE='28000'; END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended('okg.external_action.deployment:' || bound_deployment, 0));
  PERFORM okg.external_action_nomos_authority_snapshot(bound_deployment,p_command->>'authentication_audit_id',p_command->>'nomos_decision_id','external_action.execute',p_command->>'request_hash');
  SELECT * INTO proposal_row FROM okg.external_action_proposals WHERE deployment_name=bound_deployment AND proposal_id=p_command->>'proposal_id' FOR UPDATE;
  IF proposal_row.proposal_id IS NULL THEN RAISE EXCEPTION 'external action proposal was not found' USING ERRCODE='P0002'; END IF;
  -- The worker request hash is over the approved fields plus the exact
  -- retained action binding.  A direct mapped-login caller cannot replace
  -- those support digests or replay an invented claim.
  IF NOT (proposal_row.action_contract_record ? 'outcome_reason_rules') THEN
    RAISE EXCEPTION 'external action contract authority is unavailable'
      USING ERRCODE='40001';
  END IF;
  IF p_command->>'supported_action_schema_digest' IS DISTINCT FROM proposal_row.action_schema_digest
     OR p_command->>'supported_action_contract_digest' IS DISTINCT FROM proposal_row.action_contract_digest
     OR p_command->>'supported_executor_contract_digest' IS DISTINCT FROM proposal_row.executor_contract_digest
     OR p_command->>'supported_payload_renderer_digest' IS DISTINCT FROM proposal_row.payload_renderer_digest
     OR p_command->>'supported_governed_tool_binding_digest' IS DISTINCT FROM proposal_row.governed_tool_binding_digest
     OR p_command->>'supported_provider_connection_contract_digest' IS DISTINCT FROM proposal_row.provider_connection_contract_digest
     OR p_command->>'supported_provider_capability_digest' IS DISTINCT FROM proposal_row.provider_capability_digest
     OR p_command->>'supported_provider_operation_id' IS DISTINCT FROM proposal_row.provider_operation_id
     OR p_command->>'supported_provider_operation_descriptor_digest' IS DISTINCT FROM proposal_row.provider_operation_descriptor_digest THEN
    RAISE EXCEPTION 'worker effect binding is not supported' USING ERRCODE='40001';
  END IF;
    canonical_claim_request_hash:=okg.external_action_hash(
      'okg-external-action-transition-request-v1',jsonb_build_object(
        'worker_separation_subject',p_command->>'worker_separation_subject',
        'decision_id',p_command->>'decision_id',
        'approved_lifecycle_version',(p_command->>'approved_lifecycle_version')::bigint,
        'approved_payload_version',(p_command->>'approved_payload_version')::bigint,
        'approved_payload_hash',p_command->>'approved_payload_hash',
        'supported_action_schema_digest',p_command->>'supported_action_schema_digest',
        'supported_action_contract_digest',p_command->>'supported_action_contract_digest',
        'supported_executor_contract_digest',p_command->>'supported_executor_contract_digest',
        'supported_payload_renderer_digest',p_command->>'supported_payload_renderer_digest',
        'supported_governed_tool_binding_digest',p_command->>'supported_governed_tool_binding_digest',
        'supported_provider_connection_contract_digest',p_command->>'supported_provider_connection_contract_digest',
        'supported_provider_capability_digest',p_command->>'supported_provider_capability_digest',
        'supported_provider_operation_id',p_command->>'supported_provider_operation_id',
        'supported_provider_operation_descriptor_digest',p_command->>'supported_provider_operation_descriptor_digest',
        'if_match',p_command->>'if_match'));
    IF p_command->>'if_match' IS DISTINCT FROM format(
         '"lifecycle:%s;payload-version:%s;payload:%s"',
         p_command->>'approved_lifecycle_version',
         p_command->>'approved_payload_version',
         p_command->>'approved_payload_hash'
       )
       OR p_command->>'request_hash' IS DISTINCT FROM canonical_claim_request_hash THEN
      RAISE EXCEPTION 'claim request hash is not canonical' USING ERRCODE='40001';
    END IF;
  SELECT * INTO existing_execution FROM okg.external_action_executions WHERE deployment_name=bound_deployment AND proposal_id=proposal_row.proposal_id FOR UPDATE;
  IF existing_execution.execution_id IS NOT NULL THEN
    IF existing_execution.store_record->>'idempotency_key_hash'=p_command->>'idempotency_key_hash' AND existing_execution.request_hash=p_command->>'request_hash' THEN
      SELECT * INTO existing_event FROM okg.external_action_events WHERE deployment_name=bound_deployment AND execution_id=existing_execution.execution_id AND event_type='execution_claimed' ORDER BY sequence DESC LIMIT 1;
      IF existing_event.event_id IS NULL OR p_command->>'event_hash' IS DISTINCT FROM existing_event.event_hash OR p_command->>'previous_event_hash' IS DISTINCT FROM existing_event.previous_event_hash OR (p_command->>'claim_lifecycle_version')::bigint<>existing_event.lifecycle_version OR (p_command->>'event_sequence')::bigint<>existing_event.sequence OR p_command->>'claim_event_hash' IS DISTINCT FROM existing_event.event_hash OR p_command->>'event_chain_head_hash' IS DISTINCT FROM existing_event.event_hash OR p_command->>'claimed_at' IS DISTINCT FROM to_char(existing_event.recorded_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"') THEN
        RAISE EXCEPTION 'prepared claim replay does not match the retained claim event' USING ERRCODE='40001';
      END IF;
      RETURN jsonb_build_object(
        'replay',TRUE,'deployment',bound_deployment,
        'proposal_id',existing_execution.proposal_id,
        'execution_id',existing_execution.execution_id,
        'decision_id',existing_execution.decision_id,
        'execution_state','claimed',
        'execution_binding_hash',existing_execution.execution_binding_hash,
        'provider_idempotency_key',existing_execution.store_record->>'provider_idempotency_key',
        'provider_idempotency_key_digest',existing_execution.provider_idempotency_key_digest,
        'okg_claim_handoff_receipt_digest',existing_execution.okg_claim_handoff_receipt_digest,
        'handoff_protected_b64',existing_execution.handoff_protected_b64,
        'handoff_payload_b64',existing_execution.handoff_payload_b64,
        'handoff_signature_b64',existing_execution.handoff_signature_b64,
        'canonical_payload',jsonb_build_object(
          '__okg_bytes_b64',replace(
            encode(existing_execution.canonical_payload,'base64'),E'\n',''
          )
        )
      );
    END IF;
    RAISE EXCEPTION 'external action proposal has already been claimed' USING ERRCODE='40001';
  END IF;
  -- Claiming mutates the proposal and creates an execution. Enforce the
  -- persisted startup fence before either mutation; the insert trigger is a
  -- second, row-level guard for the execution insert.
  SELECT * INTO fence_row
    FROM okg.external_action_restore_fences
   WHERE deployment_name = bound_deployment
   FOR SHARE;
  IF fence_row.deployment_name IS NOT NULL
     AND (fence_row.service_mode <> 'normal'
          OR fence_row.quarantine_state <> 'clear') THEN
    RAISE EXCEPTION 'external action lifecycle is quarantined for restore'
      USING ERRCODE='40001';
  END IF;
  IF fence_row.deployment_name IS NOT NULL
     AND proposal_row.execution_epoch < fence_row.highest_accepted_epoch THEN
    RAISE EXCEPTION 'external action lifecycle epoch is stale'
      USING ERRCODE='40001';
  END IF;
  claimed_at_value := (p_command->>'claimed_at')::timestamptz;
  observed_at := claimed_at_value;
  prepared_claim_lifecycle_version := (p_command->>'claim_lifecycle_version')::bigint;
  prepared_event_sequence := (p_command->>'event_sequence')::bigint;
  IF p_command->>'claimed_at' !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z$' THEN RAISE EXCEPTION 'prepared claim timestamp is not canonical UTC' USING ERRCODE='22023'; END IF;
  -- An expired proposal is refused with a closed result, not an exception,
  -- so the caller can tell a passed deadline from an unavailable database.
  IF proposal_row.review_state='expired' THEN
    RETURN jsonb_build_object('refusal','proposal_expired','proposal_id',proposal_row.proposal_id);
  END IF;
  IF proposal_row.review_state<>'approved' OR proposal_row.execution_state<>'ready' THEN RAISE EXCEPTION 'external action proposal is not ready to claim' USING ERRCODE='40001'; END IF;
  IF proposal_row.execute_before IS NULL OR clock_timestamp()>=proposal_row.execute_before OR claimed_at_value>=proposal_row.execute_before THEN
    -- Record the elapsed execution deadline and RETURN the refusal: a raise
    -- here would roll back the approval_expired event with the transaction
    -- (issue #1925).  A NULL deadline, or a prepared claimed_at past a
    -- deadline the database clock has not reached, stays the generic refusal.
    IF proposal_row.execute_before IS NOT NULL AND clock_timestamp()>=proposal_row.execute_before THEN
      expiry_value:=okg.external_action_materialize_expiry(proposal_row.proposal_id);
      IF (expiry_value->>'expired')::boolean IS TRUE THEN
        RETURN jsonb_build_object('refusal','proposal_expired','proposal_id',proposal_row.proposal_id);
      END IF;
    END IF;
    RAISE EXCEPTION 'external action execution deadline has passed' USING ERRCODE='22008';
  END IF;
  IF proposal_row.current_decision_id<>p_command->>'decision_id' OR proposal_row.current_decision_binding_hash IS NULL THEN RAISE EXCEPTION 'external action approval binding is stale' USING ERRCODE='40001'; END IF;
  SELECT * INTO decision_row FROM okg.external_action_decisions WHERE deployment_name=bound_deployment AND decision_id=p_command->>'decision_id' AND proposal_id=proposal_row.proposal_id FOR SHARE;
  IF decision_row.decision_id IS NULL OR decision_row.verdict<>'approved' OR decision_row.decision_binding_hash<>proposal_row.current_decision_binding_hash THEN RAISE EXCEPTION 'external action approval decision is unavailable' USING ERRCODE='40001'; END IF;
  IF p_command->>'policy_digest' IS DISTINCT FROM decision_row.policy_digest THEN RAISE EXCEPTION 'external action policy binding is stale' USING ERRCODE='40001'; END IF;
  IF (p_command->>'approved_lifecycle_version')::bigint<>proposal_row.lifecycle_version OR (p_command->>'approved_payload_version')::bigint<>proposal_row.payload_version OR p_command->>'approved_payload_hash'<>proposal_row.current_payload_hash THEN RAISE EXCEPTION 'external action approval payload binding is stale' USING ERRCODE='40001'; END IF;
  SELECT envelope_digest INTO lease_digest_value FROM okg.external_action_execution_epoch_leases WHERE deployment_name=bound_deployment AND epoch=proposal_row.execution_epoch ORDER BY lease_sequence DESC LIMIT 1;
  IF lease_digest_value IS NULL OR lease_digest_value<>p_command->>'execution_epoch_lease_digest' THEN RAISE EXCEPTION 'external action execution epoch lease is stale' USING ERRCODE='40001'; END IF;
  execution_binding_projection := jsonb_build_object(
    'deployment',bound_deployment,'execution_epoch',proposal_row.execution_epoch,
    'execution_epoch_lease_digest',lease_digest_value,
    'effect_instance_key',proposal_row.effect_instance_key,
    'proposal_id',proposal_row.proposal_id,'decision_id',decision_row.decision_id,
    'execution_id',p_command->>'execution_id',
    'worker_separation_subject',p_command->>'worker_separation_subject',
    'approved_lifecycle_version',proposal_row.lifecycle_version,
    'approved_payload_version',proposal_row.payload_version,
    'approved_payload_hash',proposal_row.current_payload_hash,
    'action_type',proposal_row.action_type,'target_ref',proposal_row.target_ref,
    'action_schema_digest',proposal_row.action_schema_digest,
    'action_contract_digest',proposal_row.action_contract_digest,
    'executor_contract_digest',proposal_row.executor_contract_digest,
    'payload_renderer_digest',proposal_row.payload_renderer_digest,
    'governed_tool_binding_digest',proposal_row.governed_tool_binding_digest,
    'provider_connection_contract_digest',proposal_row.provider_connection_contract_digest,
    'provider_capability_digest',proposal_row.provider_capability_digest,
    'provider_operation_id',proposal_row.provider_operation_id,
    'provider_operation_descriptor_digest',proposal_row.provider_operation_descriptor_digest,
    'policy_digest',decision_row.policy_digest
  );
  provider_idempotency_projection := jsonb_build_object(
    'deployment',bound_deployment,'effect_instance_key',proposal_row.effect_instance_key,
    'approved_payload_hash',proposal_row.current_payload_hash,
    'action_contract_digest',proposal_row.action_contract_digest,
    'provider_connection_contract_digest',proposal_row.provider_connection_contract_digest,
    'provider_operation_id',proposal_row.provider_operation_id,
    'provider_operation_descriptor_digest',proposal_row.provider_operation_descriptor_digest
  );
  provider_idempotency_key_value := okg.external_action_effect_key(provider_idempotency_projection);
  provider_idempotency_digest_value := okg.external_action_hash('okg-external-action-effect-v1',provider_idempotency_projection);
  IF p_command->>'execution_binding_hash' IS DISTINCT FROM okg.external_action_hash('okg-external-action-execution-binding-v1',execution_binding_projection)
     OR p_command->>'provider_idempotency_key' IS DISTINCT FROM provider_idempotency_key_value
     OR p_command->>'provider_idempotency_key_digest' IS DISTINCT FROM provider_idempotency_digest_value THEN
    RAISE EXCEPTION 'caller-supplied execution or provider binding disagrees with the closed projection' USING ERRCODE='22023';
  END IF;
  IF p_command->>'okg_claim_handoff_receipt_digest' IS DISTINCT FROM
       okg.external_action_hash(
         'okg-external-action-claim-handoff-receipt-envelope-v1',
         jsonb_build_object(
           'protected_b64',p_command->>'handoff_protected_b64',
           'payload_b64',p_command->>'handoff_payload_b64',
           'signature_b64',p_command->>'handoff_signature_b64'
         )
       ) THEN
    RAISE EXCEPTION 'claim handoff envelope digest is not canonical' USING ERRCODE='22023';
  END IF;
  SELECT * INTO payload_row FROM okg.external_action_payload_versions WHERE deployment_name=bound_deployment AND proposal_id=proposal_row.proposal_id AND payload_version=proposal_row.payload_version FOR SHARE;
  IF payload_row.payload_version IS NULL THEN RAISE EXCEPTION 'external action payload is unavailable' USING ERRCODE='40001'; END IF;
  SELECT COALESCE(max(sequence),0)+1,(array_agg(event_hash ORDER BY sequence))[array_length(array_agg(event_hash ORDER BY sequence),1)] INTO event_sequence,previous_event_hash_value FROM okg.external_action_events WHERE deployment_name=bound_deployment AND effect_instance_key=proposal_row.effect_instance_key;
  SELECT event_hash INTO decision_event_hash_value FROM okg.external_action_events WHERE deployment_name=bound_deployment AND effect_instance_key=proposal_row.effect_instance_key AND event_type='approved' AND decision_id=decision_row.decision_id ORDER BY sequence DESC LIMIT 1;
  IF decision_event_hash_value IS NULL THEN RAISE EXCEPTION 'external action approval event is unavailable' USING ERRCODE='40001'; END IF;
  IF prepared_event_sequence<>event_sequence OR p_command->>'previous_event_hash' IS DISTINCT FROM previous_event_hash_value OR p_command->>'decision_event_hash' IS DISTINCT FROM decision_event_hash_value OR prepared_claim_lifecycle_version<>proposal_row.lifecycle_version+1 THEN RAISE EXCEPTION 'prepared claim event is stale' USING ERRCODE='40001'; END IF;
  INSERT INTO okg.external_action_executions (
    execution_id,deployment_name,proposal_id,decision_id,execution_epoch,execution_epoch_lease_digest,
    effect_instance_key,worker_separation_subject,approved_lifecycle_version,approved_payload_version,
    approved_payload_hash,action_type,target_ref,action_schema_digest,action_contract_digest,
    executor_contract_digest,payload_renderer_digest,governed_tool_binding_digest,
    provider_connection_contract_digest,provider_capability_digest,provider_operation_id,
    provider_operation_descriptor_digest,policy_digest,execution_binding_hash,request_hash,
    okg_claim_handoff_receipt_digest,handoff_protected_b64,handoff_payload_b64,handoff_signature_b64,
    canonical_payload,provider_idempotency_key_digest,authentication_audit_id,nomos_decision_id,
    claimed_at,execute_before,store_record
  ) VALUES (
    p_command->>'execution_id',bound_deployment,proposal_row.proposal_id,decision_row.decision_id,
    proposal_row.execution_epoch,lease_digest_value,proposal_row.effect_instance_key,
    p_command->>'worker_separation_subject',proposal_row.lifecycle_version,proposal_row.payload_version,
    proposal_row.current_payload_hash,proposal_row.action_type,proposal_row.target_ref,
    proposal_row.action_schema_digest,proposal_row.action_contract_digest,
    proposal_row.executor_contract_digest,proposal_row.payload_renderer_digest,
    proposal_row.governed_tool_binding_digest,proposal_row.provider_connection_contract_digest,
    proposal_row.provider_capability_digest,proposal_row.provider_operation_id,
    proposal_row.provider_operation_descriptor_digest,decision_row.policy_digest,
    p_command->>'execution_binding_hash',p_command->>'request_hash',
    p_command->>'okg_claim_handoff_receipt_digest',p_command->>'handoff_protected_b64',
    p_command->>'handoff_payload_b64',p_command->>'handoff_signature_b64',
    payload_row.canonical_payload,provider_idempotency_digest_value,
    p_command->>'authentication_audit_id',p_command->>'nomos_decision_id',
    claimed_at_value,proposal_row.execute_before,
    p_command || jsonb_build_object('idempotency_key_hash',p_command->>'idempotency_key_hash')
  ) RETURNING * INTO execution_row;
  UPDATE okg.external_action_proposals SET execution_state='claimed',lifecycle_version=lifecycle_version+1,updated_at=claimed_at_value WHERE deployment_name=bound_deployment AND proposal_id=proposal_row.proposal_id RETURNING * INTO proposal_row;
  event_projection := jsonb_build_object(
    'proposal_id', proposal_row.proposal_id,
    'execution_epoch', proposal_row.execution_epoch,
    'effect_instance_key', proposal_row.effect_instance_key,
    'sequence', event_sequence,
    'lifecycle_version', proposal_row.lifecycle_version,
    'event_type', 'execution_claimed',
    'actor_kind', 'authenticated_principal',
    'actor_principal', p_command ->> 'worker_principal',
    'actor_separation_subject', p_command ->> 'worker_separation_subject',
    'authentication_audit_ref', p_command ->> 'authentication_audit_id',
    'nomos_decision_ref', p_command ->> 'nomos_decision_id',
    'deadline_kind', NULL::TEXT,
    'policy_digest', decision_row.policy_digest,
    'before_review_state', 'approved',
    'after_review_state', 'approved',
    'before_execution_state', 'ready',
    'after_execution_state', 'claimed',
    'payload_version', proposal_row.payload_version,
    'payload_hash', proposal_row.current_payload_hash,
    'decision_id', decision_row.decision_id,
    'decision_binding_hash', decision_row.decision_binding_hash,
    'execution_id', execution_row.execution_id,
    'execution_binding_hash', p_command ->> 'execution_binding_hash',
    'transition_reason_code', NULL::TEXT,
    'outcome', NULL::TEXT,
    'outcome_reason_code', NULL::TEXT,
    'outcome_request_hash', NULL::TEXT,
    'restore_recovery_digest', NULL::TEXT,
    'effective_at', NULL::TEXT,
    'recorded_at', to_char(claimed_at_value AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
    'previous_event_hash', previous_event_hash_value
  );
  event_hash_value := okg.external_action_hash('okg-external-action-event-v1', event_projection);
  claim_event_hash_value := event_hash_value;
  IF p_command ->> 'event_hash' IS DISTINCT FROM event_hash_value
     OR p_command ->> 'claim_event_hash' IS DISTINCT FROM event_hash_value
     OR p_command ->> 'event_chain_head_hash' IS DISTINCT FROM event_hash_value THEN
    RAISE EXCEPTION 'caller-supplied event hash disagrees with the closed event projection'
      USING ERRCODE = '22023';
  END IF;
  INSERT INTO okg.external_action_events (
    deployment_name,proposal_id,execution_epoch,effect_instance_key,sequence,lifecycle_version,
    event_type,actor_kind,actor_principal,actor_separation_subject,authentication_audit_ref,
    nomos_decision_ref,policy_digest,before_review_state,after_review_state,
    before_execution_state,after_execution_state,payload_version,payload_hash,decision_id,
    decision_binding_hash,execution_id,execution_binding_hash,effective_at,recorded_at,
    previous_event_hash,event_hash,store_record
  ) VALUES (
    bound_deployment,proposal_row.proposal_id,proposal_row.execution_epoch,proposal_row.effect_instance_key,
    event_sequence,proposal_row.lifecycle_version,'execution_claimed','authenticated_principal',
    p_command->>'worker_principal',p_command->>'worker_separation_subject',
    p_command->>'authentication_audit_id',p_command->>'nomos_decision_id',decision_row.policy_digest,
    'approved','approved','ready','claimed',proposal_row.payload_version,proposal_row.current_payload_hash,
    decision_row.decision_id,decision_row.decision_binding_hash,execution_row.execution_id,
    p_command->>'execution_binding_hash',NULL,claimed_at_value,
    previous_event_hash_value,event_hash_value,
    jsonb_build_object('event_projection', event_projection)
  );
  RETURN jsonb_build_object(
    'replay',FALSE,'deployment',bound_deployment,
    'proposal_id',proposal_row.proposal_id,
    'execution_id',execution_row.execution_id,
    'decision_id',execution_row.decision_id,
    'execution_state','claimed',
    'lifecycle_version',proposal_row.lifecycle_version,
    'execution_binding_hash',execution_row.execution_binding_hash,
    'provider_idempotency_key',provider_idempotency_key_value,
    'provider_idempotency_key_digest',provider_idempotency_digest_value,
    'okg_claim_handoff_receipt_digest',execution_row.okg_claim_handoff_receipt_digest,
    'handoff_protected_b64',execution_row.handoff_protected_b64,
    'handoff_payload_b64',execution_row.handoff_payload_b64,
    'handoff_signature_b64',execution_row.handoff_signature_b64,
    'canonical_payload',jsonb_build_object(
      '__okg_bytes_b64',replace(
        encode(execution_row.canonical_payload,'base64'),E'\n',''
      )
    ),
    'event_hash',event_hash_value
  );
END
$external_action_claim$;
CREATE OR REPLACE FUNCTION okg.external_action_report_outcome(p_command JSONB)
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, okg AS $external_action_report_outcome$
DECLARE
  bound_deployment TEXT := okg.external_action_session_deployment();
  execution_row okg.external_action_executions%ROWTYPE;
  proposal_row okg.external_action_proposals%ROWTYPE;
  decision_row okg.external_action_decisions%ROWTYPE;
  outcome_row okg.external_action_outcomes%ROWTYPE;
  existing_outcome okg.external_action_outcomes%ROWTYPE;
  claim_auth_provider_id TEXT; claim_auth_binding_id TEXT; claim_auth_subject TEXT;
  claim_provider_status TEXT; claim_provider_updated_at TIMESTAMPTZ;
  claim_binding_status TEXT; claim_binding_updated_at TIMESTAMPTZ;
  claim_binding_subject TEXT;
  reconciler_value BOOLEAN := FALSE;
  observed_at TIMESTAMPTZ := clock_timestamp();
  event_sequence BIGINT;
  previous_event_hash_value TEXT;
  event_hash_value TEXT;
  event_projection JSONB;
  contract_rule JSONB;
  canonical_outcome_request_hash TEXT;
  audit_subject TEXT;
BEGIN
  PERFORM okg.external_action_verifier_capability_status();
  PERFORM okg.external_action_require_command(p_command,ARRAY[
    'deployment','execution_id','effect_instance_key','worker_or_reconciler_separation_subject','outcome','outcome_reason_code','provider_receipt_digest','recovery_journal_digest','owner_unavailability_attestation_digest','outcome_request_hash','idempotency_key_hash','authentication_audit_id','nomos_decision_id','policy_digest','reconciler','event_hash','previous_event_hash','if_match','expected_lifecycle_version','expected_payload_version','expected_payload_hash'
  ],ARRAY[
    'deployment','execution_id','effect_instance_key','worker_or_reconciler_separation_subject','outcome','outcome_reason_code','recovery_journal_digest','outcome_request_hash','idempotency_key_hash','authentication_audit_id','nomos_decision_id','policy_digest','if_match','expected_lifecycle_version','expected_payload_version','expected_payload_hash'
  ]);
  IF p_command->>'deployment'<>bound_deployment THEN RAISE EXCEPTION 'external action deployment is not bound to session_user' USING ERRCODE='28000'; END IF;
  reconciler_value:=COALESCE((p_command->>'reconciler')::boolean,FALSE);
  PERFORM pg_advisory_xact_lock(hashtextextended('okg.external_action.deployment:'||bound_deployment,0));
  SELECT * INTO execution_row FROM okg.external_action_executions WHERE deployment_name=bound_deployment AND execution_id=p_command->>'execution_id' FOR UPDATE;
  IF execution_row.execution_id IS NULL THEN RAISE EXCEPTION 'external action execution was not found' USING ERRCODE='P0002'; END IF;
  IF p_command->>'effect_instance_key'<>execution_row.effect_instance_key THEN RAISE EXCEPTION 'external action effect binding is stale' USING ERRCODE='40001'; END IF;
  -- Revalidate live authority before resolving an exact replay.  A replay
  -- cannot bypass current Nomos authority or claim-owner revocation.
  PERFORM okg.external_action_nomos_authority_snapshot(bound_deployment,p_command->>'authentication_audit_id',p_command->>'nomos_decision_id',CASE WHEN reconciler_value THEN 'external_action.reconcile' ELSE 'external_action.report' END,p_command->>'outcome_request_hash');
  SELECT a.separation_subject INTO audit_subject
    FROM okg.external_action_auth_audits a
   WHERE a.deployment_name=bound_deployment
     AND a.authentication_audit_id=p_command->>'authentication_audit_id';
  IF audit_subject IS NULL OR p_command->>'worker_or_reconciler_separation_subject' IS DISTINCT FROM audit_subject THEN
    RAISE EXCEPTION 'outcome actor is not the retained authenticated subject' USING ERRCODE='42501';
  END IF;
  IF NOT reconciler_value AND audit_subject<>execution_row.worker_separation_subject THEN RAISE EXCEPTION 'only the claiming worker may report the outcome' USING ERRCODE='42501'; END IF;
  IF NOT reconciler_value AND p_command->>'owner_unavailability_attestation_digest' IS NOT NULL THEN RAISE EXCEPTION 'claim-owner reports cannot include an unavailability attestation' USING ERRCODE='22023'; END IF;
  IF reconciler_value THEN
    SELECT a.nomos_provider_id,a.nomos_subject_binding_id,a.separation_subject,p.status,p.updated_at,b.status,b.updated_at,b.subject
      INTO claim_auth_provider_id,claim_auth_binding_id,claim_auth_subject,claim_provider_status,claim_provider_updated_at,claim_binding_status,claim_binding_updated_at,claim_binding_subject
      FROM okg.external_action_auth_audits a
      JOIN okg.nomos_identity_providers p ON p.deployment_name=a.deployment_name AND p.provider_id=a.nomos_provider_id
      JOIN okg.nomos_subject_bindings b ON b.deployment_name=a.deployment_name AND b.binding_id=a.nomos_subject_binding_id AND b.provider_id=p.provider_id AND b.issuer=p.issuer AND b.subject=a.separation_subject
     WHERE a.deployment_name=bound_deployment AND a.authentication_audit_id=execution_row.authentication_audit_id AND a.separation_subject=execution_row.worker_separation_subject;
    IF claim_auth_provider_id IS NULL OR claim_auth_binding_id IS NULL OR claim_auth_subject IS DISTINCT FROM execution_row.worker_separation_subject OR claim_binding_subject IS DISTINCT FROM execution_row.worker_separation_subject OR NOT ((claim_provider_status IN ('disabled','revoked') AND claim_provider_updated_at>execution_row.claimed_at) OR (claim_binding_status IN ('disabled','revoked') AND claim_binding_updated_at>execution_row.claimed_at)) THEN RAISE EXCEPTION 'reconciliation requires the exact claim identity to be disabled or revoked after claim' USING ERRCODE='42501'; END IF;
  IF EXISTS (SELECT 1 FROM okg.nomos_identity_providers p JOIN okg.nomos_subject_bindings b ON b.deployment_name=p.deployment_name AND b.provider_id=p.provider_id AND b.issuer=p.issuer WHERE p.deployment_name=bound_deployment AND b.subject=execution_row.worker_separation_subject AND NOT (p.provider_id=claim_auth_provider_id AND b.binding_id=claim_auth_binding_id) AND p.status='active' AND b.status='active' AND (p.effective_from IS NULL OR p.effective_from<=observed_at) AND (p.effective_until IS NULL OR observed_at<p.effective_until) AND (b.effective_from IS NULL OR b.effective_from<=observed_at) AND (b.effective_until IS NULL OR observed_at<b.effective_until)) THEN RAISE EXCEPTION 'reconciliation is blocked by an active replacement identity binding' USING ERRCODE='42501'; END IF;
    IF NOT (p_command ? 'owner_unavailability_attestation_digest') OR NOT EXISTS (SELECT 1 FROM okg.external_action_owner_unavailability_attestations a WHERE a.deployment_name=bound_deployment AND a.execution_id=execution_row.execution_id AND a.claim_owner_separation_subject=execution_row.worker_separation_subject AND a.envelope_digest=p_command->>'owner_unavailability_attestation_digest' AND a.reconcile_not_before<=observed_at AND observed_at<a.expires_at) THEN RAISE EXCEPTION 'reconciliation requires a current owner-unavailability attestation' USING ERRCODE='40001'; END IF;
  END IF;
  SELECT * INTO proposal_row FROM okg.external_action_proposals WHERE deployment_name=bound_deployment AND proposal_id=execution_row.proposal_id FOR SHARE;
  IF proposal_row.proposal_id IS NULL THEN RAISE EXCEPTION 'external action proposal was not found' USING ERRCODE='P0002'; END IF;
  SELECT * INTO decision_row FROM okg.external_action_decisions
   WHERE deployment_name=bound_deployment AND decision_id=execution_row.decision_id
   FOR SHARE;
  IF decision_row.decision_id IS NULL THEN
    RAISE EXCEPTION 'external action decision was not found' USING ERRCODE='P0002';
  END IF;
  IF p_command->>'policy_digest' IS DISTINCT FROM decision_row.policy_digest
     OR execution_row.policy_digest IS DISTINCT FROM decision_row.policy_digest THEN
    RAISE EXCEPTION 'external action outcome policy binding is stale' USING ERRCODE='40001';
  END IF;
  IF NOT (proposal_row.action_contract_record ? 'outcome_reason_rules') THEN
    RAISE EXCEPTION 'external action contract authority is unavailable'
      USING ERRCODE='40001';
  END IF;
    contract_rule:=proposal_row.action_contract_record->'outcome_reason_rules'->(p_command->>'outcome_reason_code');
    IF jsonb_typeof(contract_rule) IS DISTINCT FROM 'object'
       OR NOT (contract_rule->'allowed_outcomes' ? (p_command->>'outcome'))
       OR contract_rule->>'provider_receipt' NOT IN ('required','forbidden')
       OR ((contract_rule->>'provider_receipt')='required') IS DISTINCT FROM (p_command->>'provider_receipt_digest' IS NOT NULL) THEN
      RAISE EXCEPTION 'outcome is not allowed by the retained action contract' USING ERRCODE='22023';
    END IF;
  IF p_command->>'provider_receipt_digest' IS NOT NULL
     AND p_command->>'provider_receipt_digest' !~ '^sha256:[0-9a-f]{64}$' THEN
    RAISE EXCEPTION 'provider receipt digest is not canonical' USING ERRCODE='22023';
  END IF;
    canonical_outcome_request_hash:=okg.external_action_hash(
      'okg-external-action-outcome-request-v1',jsonb_build_object(
        'execution_id',execution_row.execution_id,
        'effect_instance_key',execution_row.effect_instance_key,
        'worker_or_reconciler_separation_subject',audit_subject,
        'outcome',p_command->>'outcome',
        'outcome_reason_code',p_command->>'outcome_reason_code',
        'provider_receipt_digest',p_command->>'provider_receipt_digest',
        'recovery_journal_digest',p_command->>'recovery_journal_digest',
        'owner_unavailability_attestation_digest',p_command->>'owner_unavailability_attestation_digest',
        'if_match',p_command->>'if_match'));
    IF p_command->>'outcome_request_hash' IS DISTINCT FROM canonical_outcome_request_hash THEN
      RAISE EXCEPTION 'outcome request hash is not canonical' USING ERRCODE='40001';
    END IF;
  SELECT * INTO existing_outcome FROM okg.external_action_outcomes WHERE deployment_name=bound_deployment AND execution_id=execution_row.execution_id FOR UPDATE;
  IF existing_outcome.outcome_id IS NOT NULL THEN
    IF existing_outcome.request_hash=p_command->>'outcome_request_hash' AND existing_outcome.store_record->>'idempotency_key_hash'=p_command->>'idempotency_key_hash' THEN
      RETURN jsonb_build_object('replay',TRUE,'deployment',bound_deployment,'execution_id',existing_outcome.execution_id,'effect_instance_key',existing_outcome.effect_instance_key,'outcome',existing_outcome.outcome,'outcome_request_hash',existing_outcome.request_hash);
    END IF;
    RAISE EXCEPTION 'external action outcome has already been recorded' USING ERRCODE='40001';
  END IF;
  IF p_command->>'if_match' IS DISTINCT FROM format(
       '"lifecycle:%s;payload-version:%s;payload:%s"',
       p_command->>'expected_lifecycle_version',
       p_command->>'expected_payload_version',
       p_command->>'expected_payload_hash'
     )
     OR (p_command->>'expected_lifecycle_version')::bigint IS DISTINCT FROM proposal_row.lifecycle_version
     OR (p_command->>'expected_payload_version')::bigint IS DISTINCT FROM proposal_row.payload_version
     OR p_command->>'expected_payload_hash' IS DISTINCT FROM proposal_row.current_payload_hash THEN
    RAISE EXCEPTION 'external action outcome precondition is stale'
      USING ERRCODE='40001';
  END IF;
  IF reconciler_value AND (p_command->>'worker_or_reconciler_separation_subject'=execution_row.worker_separation_subject OR p_command->>'outcome'<>'indeterminate' OR NOT (p_command ? 'owner_unavailability_attestation_digest')) THEN RAISE EXCEPTION 'reconciliation requires an independent indeterminate outcome' USING ERRCODE='42501'; END IF;
  IF p_command->>'outcome' NOT IN ('succeeded','failed','indeterminate') THEN RAISE EXCEPTION 'external action outcome is not allowed' USING ERRCODE='22023'; END IF;
  IF p_command->>'outcome'='succeeded' AND NOT (p_command ? 'provider_receipt_digest') THEN RAISE EXCEPTION 'successful outcome requires a provider receipt' USING ERRCODE='22023'; END IF;
  IF reconciler_value AND p_command ? 'provider_receipt_digest' THEN RAISE EXCEPTION 'reconciliation cannot provide a provider receipt' USING ERRCODE='22023'; END IF;
  IF p_command->>'owner_unavailability_attestation_digest' IS NOT NULL AND NOT reconciler_value AND NOT EXISTS (SELECT 1 FROM okg.external_action_owner_unavailability_attestations a WHERE a.deployment_name=bound_deployment AND a.execution_id=execution_row.execution_id AND a.envelope_digest=p_command->>'owner_unavailability_attestation_digest') THEN RAISE EXCEPTION 'owner-unavailability attestation is not bound' USING ERRCODE='40001'; END IF;
  SELECT * INTO proposal_row FROM okg.external_action_proposals WHERE deployment_name=bound_deployment AND proposal_id=execution_row.proposal_id FOR UPDATE;
  INSERT INTO okg.external_action_outcomes (deployment_name,execution_id,effect_instance_key,worker_or_reconciler_separation_subject,outcome,outcome_reason_code,provider_receipt_digest,recovery_journal_digest,owner_unavailability_attestation_digest,request_hash,authentication_audit_id,nomos_decision_id,recorded_at,store_record) VALUES (bound_deployment,execution_row.execution_id,execution_row.effect_instance_key,p_command->>'worker_or_reconciler_separation_subject',p_command->>'outcome',p_command->>'outcome_reason_code',p_command->>'provider_receipt_digest',p_command->>'recovery_journal_digest',p_command->>'owner_unavailability_attestation_digest',p_command->>'outcome_request_hash',p_command->>'authentication_audit_id',p_command->>'nomos_decision_id',observed_at,p_command||jsonb_build_object('idempotency_key_hash',p_command->>'idempotency_key_hash')) RETURNING * INTO outcome_row;
  UPDATE okg.external_action_proposals SET execution_state=p_command->>'outcome',lifecycle_version=lifecycle_version+1,updated_at=observed_at WHERE deployment_name=bound_deployment AND proposal_id=proposal_row.proposal_id RETURNING * INTO proposal_row;
  SELECT COALESCE(max(sequence),0)+1,(array_agg(event_hash ORDER BY sequence))[array_length(array_agg(event_hash ORDER BY sequence),1)] INTO event_sequence,previous_event_hash_value FROM okg.external_action_events WHERE deployment_name=bound_deployment AND effect_instance_key=execution_row.effect_instance_key;
  IF p_command->>'previous_event_hash' IS NOT NULL AND p_command->>'previous_event_hash' IS DISTINCT FROM previous_event_hash_value THEN RAISE EXCEPTION 'external action event chain predecessor is stale' USING ERRCODE='40001'; END IF;
  event_projection := jsonb_build_object(
    'proposal_id', proposal_row.proposal_id,
    'execution_epoch', proposal_row.execution_epoch,
    'effect_instance_key', execution_row.effect_instance_key,
    'sequence', event_sequence,
    'lifecycle_version', proposal_row.lifecycle_version,
    'event_type', 'outcome_reported',
    'actor_kind', 'authenticated_principal',
    'actor_principal', NULL::TEXT,
    'actor_separation_subject', p_command->>'worker_or_reconciler_separation_subject',
    'authentication_audit_ref', p_command->>'authentication_audit_id',
    'nomos_decision_ref', p_command->>'nomos_decision_id',
    'deadline_kind', NULL::TEXT,
    'policy_digest', decision_row.policy_digest,
    'before_review_state', proposal_row.review_state,
    'after_review_state', proposal_row.review_state,
    'before_execution_state', 'claimed',
    'after_execution_state', p_command->>'outcome',
    'payload_version', proposal_row.payload_version,
    'payload_hash', proposal_row.current_payload_hash,
    'decision_id', execution_row.decision_id,
    'decision_binding_hash', decision_row.decision_binding_hash,
    'execution_id', execution_row.execution_id,
    'execution_binding_hash', execution_row.execution_binding_hash,
    'transition_reason_code', NULL::TEXT,
    'outcome', p_command->>'outcome',
    'outcome_reason_code', p_command->>'outcome_reason_code',
    'outcome_request_hash', p_command->>'outcome_request_hash',
    'restore_recovery_digest', NULL::TEXT,
    'effective_at', NULL::TEXT,
    'recorded_at', to_char(observed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
    'previous_event_hash', previous_event_hash_value
  );
  event_hash_value:=okg.external_action_hash('okg-external-action-event-v1',event_projection);
  IF p_command->>'event_hash' IS NOT NULL
     AND p_command->>'event_hash' IS DISTINCT FROM event_hash_value THEN
    RAISE EXCEPTION 'caller-supplied event hash disagrees with the closed event projection' USING ERRCODE='22023';
  END IF;
  INSERT INTO okg.external_action_events (deployment_name,proposal_id,execution_epoch,effect_instance_key,sequence,lifecycle_version,event_type,actor_kind,actor_principal,actor_separation_subject,authentication_audit_ref,nomos_decision_ref,deadline_kind,policy_digest,before_review_state,after_review_state,before_execution_state,after_execution_state,payload_version,payload_hash,decision_id,decision_binding_hash,execution_id,execution_binding_hash,transition_reason_code,outcome,outcome_reason_code,outcome_request_hash,restore_recovery_digest,effective_at,recorded_at,previous_event_hash,event_hash,store_record) VALUES (bound_deployment,proposal_row.proposal_id,proposal_row.execution_epoch,execution_row.effect_instance_key,event_sequence,proposal_row.lifecycle_version,'outcome_reported','authenticated_principal',NULL,p_command->>'worker_or_reconciler_separation_subject',p_command->>'authentication_audit_id',p_command->>'nomos_decision_id',NULL,decision_row.policy_digest,proposal_row.review_state,proposal_row.review_state,'claimed',p_command->>'outcome',proposal_row.payload_version,proposal_row.current_payload_hash,execution_row.decision_id,decision_row.decision_binding_hash,execution_row.execution_id,execution_row.execution_binding_hash,NULL,p_command->>'outcome',p_command->>'outcome_reason_code',p_command->>'outcome_request_hash',NULL,NULL,observed_at,previous_event_hash_value,event_hash_value,jsonb_build_object('event_projection',event_projection));
  RETURN jsonb_build_object('replay',FALSE,'deployment',bound_deployment,'execution_id',execution_row.execution_id,'effect_instance_key',execution_row.effect_instance_key,'outcome',outcome_row.outcome,'outcome_request_hash',outcome_row.request_hash,'event_hash',event_hash_value);
END
$external_action_report_outcome$;

CREATE OR REPLACE FUNCTION okg.external_action_owner_unavailability(p_command JSONB)
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, okg AS $external_action_owner_unavailability$
DECLARE
  bound_deployment TEXT := okg.external_action_session_deployment();
  execution_row okg.external_action_executions%ROWTYPE;
  proposal_row okg.external_action_proposals%ROWTYPE;
  attestation_row okg.external_action_owner_unavailability_attestations%ROWTYPE;
  existing_row okg.external_action_owner_unavailability_attestations%ROWTYPE;
  observed_at TIMESTAMPTZ := clock_timestamp();
  audit_subject TEXT;
  expected_reconcile_not_before TIMESTAMPTZ;
  expected_request_hash TEXT;
  expected_envelope_digest TEXT;
  expected_response_body BYTEA;
BEGIN
  PERFORM okg.external_action_verifier_capability_status();
  PERFORM okg.external_action_require_command(p_command,ARRAY['deployment','execution_id','claim_owner_separation_subject','reconcile_not_before','issued_at','expires_at','nonce','signer_separation_subject','authenticated_submitter_separation_subject','idempotency_key_hash','request_hash','response_status','response_body_jcs','protected_b64','payload_b64','signature_b64','signer_key_id_digest','envelope_digest','authentication_audit_id','nomos_decision_id'],ARRAY['deployment','execution_id','claim_owner_separation_subject','reconcile_not_before','issued_at','expires_at','nonce','signer_separation_subject','authenticated_submitter_separation_subject','idempotency_key_hash','request_hash','response_status','response_body_jcs','protected_b64','payload_b64','signature_b64','signer_key_id_digest','envelope_digest','authentication_audit_id','nomos_decision_id']);
  IF p_command->>'deployment'<>bound_deployment THEN RAISE EXCEPTION 'external action deployment is not bound to session_user' USING ERRCODE='28000'; END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended('okg.external_action.deployment:'||bound_deployment,0));
  SELECT * INTO execution_row FROM okg.external_action_executions WHERE deployment_name=bound_deployment AND execution_id=p_command->>'execution_id' FOR SHARE;
  IF execution_row.execution_id IS NULL THEN RAISE EXCEPTION 'external action execution was not found' USING ERRCODE='P0002'; END IF;
  SELECT * INTO proposal_row FROM okg.external_action_proposals
   WHERE deployment_name=bound_deployment AND proposal_id=execution_row.proposal_id
   FOR SHARE;
  IF proposal_row.proposal_id IS NULL
     OR NOT (proposal_row.action_contract_record ? 'review_and_execution_limits') THEN
    RAISE EXCEPTION 'external action contract authority is unavailable' USING ERRCODE='40001';
  END IF;
  PERFORM okg.external_action_nomos_authority_snapshot(bound_deployment,p_command->>'authentication_audit_id',p_command->>'nomos_decision_id','external_action.attest_owner_unavailable',p_command->>'request_hash');
  SELECT separation_subject INTO audit_subject
    FROM okg.external_action_auth_audits
   WHERE deployment_name=bound_deployment
     AND authentication_audit_id=p_command->>'authentication_audit_id';
  expected_reconcile_not_before := execution_row.claimed_at + make_interval(
    secs => (proposal_row.action_contract_record->'review_and_execution_limits'->>'reconcile_not_before_seconds')::double precision
  );
  expected_request_hash := okg.external_action_hash(
    'okg-external-action-transition-request-v1',
    jsonb_build_object(
      'protected_b64',p_command->>'protected_b64',
      'payload_b64',p_command->>'payload_b64',
      'signature_b64',p_command->>'signature_b64'
    )
  );
  expected_envelope_digest := okg.external_action_hash(
    'okg-external-action-owner-unavailability-attestation-envelope-v1',
    jsonb_build_object(
      'protected_b64',p_command->>'protected_b64',
      'payload_b64',p_command->>'payload_b64',
      'signature_b64',p_command->>'signature_b64'
    )
  );
  expected_response_body := convert_to(
    okg.jsonb_canonical_text(jsonb_build_object(
      'protected_b64',p_command->>'protected_b64',
      'payload_b64',p_command->>'payload_b64',
      'signature_b64',p_command->>'signature_b64',
      'envelope_digest',expected_envelope_digest
    )),
    'UTF8'
  );
  IF audit_subject IS NULL
     OR p_command->>'authenticated_submitter_separation_subject' IS DISTINCT FROM audit_subject
     OR p_command->>'signer_separation_subject' IS DISTINCT FROM audit_subject
     OR p_command->>'claim_owner_separation_subject' IS DISTINCT FROM execution_row.worker_separation_subject
     OR p_command->>'signer_separation_subject'=execution_row.worker_separation_subject THEN
    RAISE EXCEPTION 'owner-unavailability attestation subject binding is invalid' USING ERRCODE='42501';
  END IF;
  IF p_command->>'request_hash' IS DISTINCT FROM expected_request_hash
     OR p_command->>'envelope_digest' IS DISTINCT FROM expected_envelope_digest
     OR (p_command->>'response_status')::integer<>201
     OR okg.external_action_json_bytes(p_command->'response_body_jcs') IS DISTINCT FROM expected_response_body THEN
    RAISE EXCEPTION 'owner-unavailability attestation evidence is not canonical' USING ERRCODE='22023';
  END IF;
  IF p_command->>'reconcile_not_before' !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z$'
     OR p_command->>'issued_at' !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z$'
     OR p_command->>'expires_at' !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z$'
     OR (p_command->>'reconcile_not_before')::timestamptz IS DISTINCT FROM expected_reconcile_not_before
     OR (p_command->>'issued_at')::timestamptz<expected_reconcile_not_before
     OR (p_command->>'issued_at')::timestamptz>observed_at
     OR (p_command->>'expires_at')::timestamptz<=(p_command->>'issued_at')::timestamptz
     OR (p_command->>'expires_at')::timestamptz-(p_command->>'issued_at')::timestamptz>interval '1 day'
     OR p_command->>'nonce' !~ '^[A-Za-z0-9._:+/\-]{16,128}$' THEN
    RAISE EXCEPTION 'owner-unavailability attestation validity interval is invalid' USING ERRCODE='22023';
  END IF;
  SELECT * INTO existing_row FROM okg.external_action_owner_unavailability_attestations WHERE deployment_name=bound_deployment AND envelope_digest=p_command->>'envelope_digest' FOR UPDATE;
  IF existing_row.attestation_id IS NOT NULL THEN
    IF existing_row.request_hash IS DISTINCT FROM p_command->>'request_hash'
       OR existing_row.idempotency_key_hash IS DISTINCT FROM p_command->>'idempotency_key_hash'
       OR existing_row.authenticated_submitter_separation_subject IS DISTINCT FROM audit_subject
       OR existing_row.store_record IS DISTINCT FROM p_command THEN
      RAISE EXCEPTION 'owner-unavailability attestation replay conflicts' USING ERRCODE='40001';
    END IF;
    RETURN jsonb_build_object('replay',TRUE,'deployment',bound_deployment,'execution_id',existing_row.execution_id,'envelope_digest',existing_row.envelope_digest);
  END IF;
  IF observed_at>=(p_command->>'expires_at')::timestamptz THEN
    RAISE EXCEPTION 'owner-unavailability attestation has expired' USING ERRCODE='22008';
  END IF;
  INSERT INTO okg.external_action_owner_unavailability_attestations (deployment_name,execution_id,claim_owner_separation_subject,reconcile_not_before,issued_at,expires_at,nonce,signer_separation_subject,authenticated_submitter_separation_subject,idempotency_key_hash,request_hash,response_status,response_body_jcs,protected_b64,payload_b64,signature_b64,signer_key_id_digest,envelope_digest,verified_at,authentication_audit_id,nomos_decision_id,store_record) VALUES (bound_deployment,p_command->>'execution_id',p_command->>'claim_owner_separation_subject',(p_command->>'reconcile_not_before')::timestamptz,(p_command->>'issued_at')::timestamptz,(p_command->>'expires_at')::timestamptz,p_command->>'nonce',p_command->>'signer_separation_subject',p_command->>'authenticated_submitter_separation_subject',p_command->>'idempotency_key_hash',p_command->>'request_hash',(p_command->>'response_status')::integer,okg.external_action_json_bytes(p_command->'response_body_jcs'),p_command->>'protected_b64',p_command->>'payload_b64',p_command->>'signature_b64',p_command->>'signer_key_id_digest',p_command->>'envelope_digest',observed_at,p_command->>'authentication_audit_id',p_command->>'nomos_decision_id',p_command) RETURNING * INTO attestation_row;
  RETURN jsonb_build_object('replay',FALSE,'deployment',bound_deployment,'execution_id',attestation_row.execution_id,'envelope_digest',attestation_row.envelope_digest,'request_hash',attestation_row.request_hash);
END
$external_action_owner_unavailability$;
CREATE OR REPLACE FUNCTION okg.external_action_restore_create(p_command JSONB)
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, okg AS $external_action_restore_create$
DECLARE bound_deployment TEXT:=okg.external_action_session_deployment(); fence_row okg.external_action_restore_fences%ROWTYPE; attestation_row okg.external_action_restore_attestations%ROWTYPE; session_row okg.external_action_restore_sessions%ROWTYPE; old_session okg.external_action_restore_sessions%ROWTYPE; completion_row okg.external_action_restore_completion_receipts%ROWTYPE; observed_at TIMESTAMPTZ:=clock_timestamp();
BEGIN
  PERFORM okg.external_action_require_command(p_command,ARRAY['deployment','restore_id','old_epoch','new_epoch','old_epoch_lease_digest','new_epoch_lease_digest','ledger_fence_envelope_digest','fenced_ledger_root_digest','fenced_ledger_entry_count','restored_snapshot_digest','pre_restore_event_root_digest','pre_restore_event_head_count','effect_inventory_digest','effect_inventory_count','worker_checkpoint_set_digest','worker_checkpoint_count','issued_at','expires_at','nonce','signer_separation_subject','protected_b64','payload_b64','signature_b64','signer_key_id_digest','envelope_digest','authentication_audit_id','nomos_decision_id','operator_separation_subject','request_hash','idempotency_key_hash','status'],ARRAY['deployment','restore_id','old_epoch','new_epoch','old_epoch_lease_digest','new_epoch_lease_digest','ledger_fence_envelope_digest','fenced_ledger_root_digest','fenced_ledger_entry_count','restored_snapshot_digest','pre_restore_event_root_digest','pre_restore_event_head_count','effect_inventory_digest','effect_inventory_count','worker_checkpoint_set_digest','worker_checkpoint_count','issued_at','expires_at','nonce','signer_separation_subject','protected_b64','payload_b64','signature_b64','signer_key_id_digest','envelope_digest','authentication_audit_id','nomos_decision_id','operator_separation_subject','request_hash','idempotency_key_hash']);
  IF p_command->>'deployment'<>bound_deployment THEN RAISE EXCEPTION 'external action deployment is not bound to session_user' USING ERRCODE='28000'; END IF;
  IF (p_command->>'new_epoch')::bigint<=(p_command->>'old_epoch')::bigint THEN RAISE EXCEPTION 'restore epoch must advance' USING ERRCODE='40001'; END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended('okg.external_action.deployment:'||bound_deployment,0));
  PERFORM okg.external_action_nomos_authority_snapshot(bound_deployment,p_command->>'authentication_audit_id',p_command->>'nomos_decision_id','external_action.restore_reconcile',p_command->>'request_hash');
  SELECT * INTO fence_row FROM okg.external_action_restore_fences WHERE deployment_name=bound_deployment FOR UPDATE;
  IF fence_row.deployment_name IS NOT NULL AND fence_row.highest_accepted_epoch>=(p_command->>'new_epoch')::bigint THEN RAISE EXCEPTION 'restore epoch is not newer than accepted fence' USING ERRCODE='40001'; END IF;
  SELECT * INTO old_session FROM okg.external_action_restore_sessions WHERE deployment_name=bound_deployment AND restore_id=p_command->>'restore_id' FOR UPDATE;
  IF old_session.restore_session_row_id IS NOT NULL THEN
    IF old_session.request_hash<>p_command->>'request_hash' THEN RAISE EXCEPTION 'restore id was reused with a different request' USING ERRCODE='40001'; END IF;
    SELECT * INTO completion_row FROM okg.external_action_restore_completion_receipts WHERE deployment_name=bound_deployment AND restore_id=old_session.restore_id FOR SHARE;
    RETURN jsonb_build_object('replay',TRUE,'deployment',bound_deployment,'restore_id',old_session.restore_id,'status',CASE WHEN completion_row.completion_receipt_row_id IS NULL THEN old_session.status ELSE 'complete' END);
  END IF;
  INSERT INTO okg.external_action_restore_attestations (deployment_name,restore_id,old_epoch,new_epoch,old_epoch_lease_digest,new_epoch_lease_digest,ledger_fence_envelope_digest,fenced_ledger_root_digest,fenced_ledger_entry_count,restored_snapshot_digest,pre_restore_event_root_digest,pre_restore_event_head_count,effect_inventory_digest,effect_inventory_count,worker_checkpoint_set_digest,worker_checkpoint_count,issued_at,expires_at,nonce,signer_separation_subject,protected_b64,payload_b64,signature_b64,signer_key_id_digest,envelope_digest,verified_at,authentication_audit_id,nomos_decision_id,store_record) VALUES (bound_deployment,p_command->>'restore_id',(p_command->>'old_epoch')::bigint,(p_command->>'new_epoch')::bigint,p_command->>'old_epoch_lease_digest',p_command->>'new_epoch_lease_digest',p_command->>'ledger_fence_envelope_digest',p_command->>'fenced_ledger_root_digest',(p_command->>'fenced_ledger_entry_count')::bigint,p_command->>'restored_snapshot_digest',p_command->>'pre_restore_event_root_digest',(p_command->>'pre_restore_event_head_count')::bigint,p_command->>'effect_inventory_digest',(p_command->>'effect_inventory_count')::bigint,p_command->>'worker_checkpoint_set_digest',(p_command->>'worker_checkpoint_count')::bigint,(p_command->>'issued_at')::timestamptz,(p_command->>'expires_at')::timestamptz,p_command->>'nonce',p_command->>'signer_separation_subject',p_command->>'protected_b64',p_command->>'payload_b64',p_command->>'signature_b64',p_command->>'signer_key_id_digest',p_command->>'envelope_digest',observed_at,p_command->>'authentication_audit_id',p_command->>'nomos_decision_id',p_command) RETURNING * INTO attestation_row;
  INSERT INTO okg.external_action_restore_sessions (deployment_name,restore_id,request_hash,idempotency_key_hash,operator_separation_subject,authentication_audit_id,nomos_decision_id,restore_attestation_envelope_digest,status,created_at,updated_at,store_record) VALUES (bound_deployment,p_command->>'restore_id',p_command->>'request_hash',p_command->>'idempotency_key_hash',p_command->>'operator_separation_subject',p_command->>'authentication_audit_id',p_command->>'nomos_decision_id',attestation_row.envelope_digest,COALESCE(p_command->>'status','staging'),observed_at,observed_at,p_command) RETURNING * INTO session_row;
  RETURN jsonb_build_object('replay',FALSE,'deployment',bound_deployment,'restore_id',session_row.restore_id,'status',session_row.status,'restore_attestation_envelope_digest',session_row.restore_attestation_envelope_digest);
END
$external_action_restore_create$;

CREATE OR REPLACE FUNCTION okg.external_action_restore_batch(p_command JSONB)
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, okg AS $external_action_restore_batch$
DECLARE bound_deployment TEXT:=okg.external_action_session_deployment(); session_row okg.external_action_restore_sessions%ROWTYPE; checkpoint_row okg.external_action_restore_checkpoint_staging%ROWTYPE; existing_row okg.external_action_restore_checkpoint_staging%ROWTYPE;
BEGIN
  PERFORM okg.external_action_require_command(p_command,ARRAY['deployment','restore_id','batch_index','request_hash','checkpoint_envelope_digest','proof_digest','checkpoint_payload','authentication_audit_id','nomos_decision_id'],ARRAY['deployment','restore_id','batch_index','request_hash','checkpoint_envelope_digest','proof_digest','checkpoint_payload','authentication_audit_id','nomos_decision_id']);
  IF p_command->>'deployment'<>bound_deployment THEN RAISE EXCEPTION 'external action deployment is not bound to session_user' USING ERRCODE='28000'; END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended('okg.external_action.deployment:'||bound_deployment,0));
  PERFORM okg.external_action_nomos_authority_snapshot(bound_deployment,p_command->>'authentication_audit_id',p_command->>'nomos_decision_id','external_action.restore_reconcile',p_command->>'request_hash');
  SELECT * INTO session_row FROM okg.external_action_restore_sessions WHERE deployment_name=bound_deployment AND restore_id=p_command->>'restore_id' FOR UPDATE;
  IF session_row.restore_session_row_id IS NULL OR session_row.status NOT IN ('staging','quarantined') THEN RAISE EXCEPTION 'restore session is not accepting checkpoint batches' USING ERRCODE='40001'; END IF;
  SELECT * INTO existing_row FROM okg.external_action_restore_checkpoint_staging WHERE deployment_name=bound_deployment AND restore_id=session_row.restore_id AND batch_index=(p_command->>'batch_index')::bigint FOR UPDATE;
  IF existing_row.checkpoint_staging_row_id IS NOT NULL THEN
    IF existing_row.request_hash<>p_command->>'request_hash' THEN RAISE EXCEPTION 'restore batch index was reused with a different request' USING ERRCODE='40001'; END IF;
    RETURN jsonb_build_object('replay',TRUE,'deployment',bound_deployment,'restore_id',session_row.restore_id,'batch_index',existing_row.batch_index,'checkpoint_envelope_digest',existing_row.checkpoint_envelope_digest);
  END IF;
  INSERT INTO okg.external_action_restore_checkpoint_staging (deployment_name,restore_id,batch_index,request_hash,checkpoint_envelope_digest,proof_digest,checkpoint_payload,staged_at,store_record) VALUES (bound_deployment,session_row.restore_id,(p_command->>'batch_index')::bigint,p_command->>'request_hash',p_command->>'checkpoint_envelope_digest',p_command->>'proof_digest',p_command->'checkpoint_payload',clock_timestamp(),p_command) RETURNING * INTO checkpoint_row;
  RETURN jsonb_build_object('replay',FALSE,'deployment',bound_deployment,'restore_id',checkpoint_row.restore_id,'batch_index',checkpoint_row.batch_index,'checkpoint_envelope_digest',checkpoint_row.checkpoint_envelope_digest);
END
$external_action_restore_batch$;

CREATE OR REPLACE FUNCTION okg.external_action_restore_finalize(p_command JSONB)
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, okg AS $external_action_restore_finalize$
DECLARE bound_deployment TEXT:=okg.external_action_session_deployment(); session_row okg.external_action_restore_sessions%ROWTYPE; completion_row okg.external_action_restore_completion_receipts%ROWTYPE; attestation_row okg.external_action_restore_attestations%ROWTYPE; fence_row okg.external_action_restore_fences%ROWTYPE; observed_at TIMESTAMPTZ:=clock_timestamp();
BEGIN
  PERFORM okg.external_action_require_command(p_command,ARRAY['deployment','restore_id','new_epoch','new_epoch_lease_digest','lease_chain_digest','verification_config_digest','verification_login_binding_digest','service_mode','quarantine_state','completion_receipt_envelope_digest','authentication_audit_id','nomos_decision_id'],ARRAY['deployment','restore_id','new_epoch','new_epoch_lease_digest','lease_chain_digest','verification_config_digest','verification_login_binding_digest','service_mode','quarantine_state','completion_receipt_envelope_digest','authentication_audit_id','nomos_decision_id']);
  IF p_command->>'deployment'<>bound_deployment THEN RAISE EXCEPTION 'external action deployment is not bound to session_user' USING ERRCODE='28000'; END IF;
  IF p_command->>'service_mode' NOT IN ('normal','restore_only') OR p_command->>'quarantine_state' NOT IN ('clear','blocked') THEN RAISE EXCEPTION 'restore final mode is not allowed' USING ERRCODE='22023'; END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended('okg.external_action.deployment:'||bound_deployment,0));
  PERFORM okg.external_action_nomos_authority_snapshot(bound_deployment,p_command->>'authentication_audit_id',p_command->>'nomos_decision_id','external_action.restore_reconcile',p_command->>'request_hash');
  SELECT * INTO session_row FROM okg.external_action_restore_sessions WHERE deployment_name=bound_deployment AND restore_id=p_command->>'restore_id' FOR UPDATE;
  IF session_row.restore_session_row_id IS NULL THEN RAISE EXCEPTION 'restore session is not finalizable' USING ERRCODE='40001'; END IF;
  SELECT * INTO completion_row FROM okg.external_action_restore_completion_receipts WHERE deployment_name=bound_deployment AND restore_id=session_row.restore_id FOR SHARE;
  IF completion_row.completion_receipt_row_id IS NOT NULL THEN
    IF completion_row.completion_receipt_envelope_digest<>p_command->>'completion_receipt_envelope_digest' THEN RAISE EXCEPTION 'restore finalization id was reused with a different completion receipt' USING ERRCODE='40001'; END IF;
    RETURN jsonb_build_object('replay',TRUE,'deployment',bound_deployment,'restore_id',session_row.restore_id,'status','complete','completion_receipt_envelope_digest',completion_row.completion_receipt_envelope_digest); END IF;
  SELECT * INTO attestation_row FROM okg.external_action_restore_attestations WHERE deployment_name=bound_deployment AND envelope_digest=session_row.restore_attestation_envelope_digest FOR SHARE;
  IF attestation_row.attestation_row_id IS NULL OR attestation_row.new_epoch<>(p_command->>'new_epoch')::bigint OR attestation_row.new_epoch_lease_digest<>p_command->>'new_epoch_lease_digest' THEN RAISE EXCEPTION 'restore attestation does not match final fence' USING ERRCODE='40001'; END IF;
  SELECT * INTO fence_row FROM okg.external_action_restore_fences WHERE deployment_name=bound_deployment FOR UPDATE;
  IF fence_row.deployment_name IS NOT NULL AND fence_row.highest_accepted_epoch>=attestation_row.new_epoch THEN RAISE EXCEPTION 'restore final epoch is stale' USING ERRCODE='40001'; END IF;
  INSERT INTO okg.external_action_restore_fences (deployment_name,highest_accepted_epoch,lease_digest,quarantine_state,service_mode,lease_chain_digest,verification_config_digest,verification_login_binding_digest,restore_attestation_digest,accepted_at,mode_changed_at,store_record) VALUES (bound_deployment,attestation_row.new_epoch,p_command->>'new_epoch_lease_digest',p_command->>'quarantine_state',p_command->>'service_mode',p_command->>'lease_chain_digest',p_command->>'verification_config_digest',p_command->>'verification_login_binding_digest',attestation_row.envelope_digest,observed_at,observed_at,p_command) ON CONFLICT (deployment_name) DO UPDATE SET highest_accepted_epoch=EXCLUDED.highest_accepted_epoch,lease_digest=EXCLUDED.lease_digest,quarantine_state=EXCLUDED.quarantine_state,service_mode=EXCLUDED.service_mode,lease_chain_digest=EXCLUDED.lease_chain_digest,verification_config_digest=EXCLUDED.verification_config_digest,verification_login_binding_digest=EXCLUDED.verification_login_binding_digest,restore_attestation_digest=EXCLUDED.restore_attestation_digest,accepted_at=EXCLUDED.accepted_at,mode_changed_at=EXCLUDED.mode_changed_at,store_record=EXCLUDED.store_record;
  INSERT INTO okg.external_action_restore_completion_receipts (deployment_name,restore_id,restore_attestation_envelope_digest,completion_receipt_envelope_digest,completed_at,authentication_audit_id,nomos_decision_id,store_record) VALUES (bound_deployment,session_row.restore_id,session_row.restore_attestation_envelope_digest,p_command->>'completion_receipt_envelope_digest',observed_at,p_command->>'authentication_audit_id',p_command->>'nomos_decision_id',p_command);
  RETURN jsonb_build_object('replay',FALSE,'deployment',bound_deployment,'restore_id',session_row.restore_id,'status','complete','highest_accepted_epoch',attestation_row.new_epoch,'restore_attestation_envelope_digest',attestation_row.envelope_digest);
END
$external_action_restore_finalize$;
CREATE OR REPLACE FUNCTION okg.external_action_store_snapshot()
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, okg AS $external_action_store_snapshot_v2$
DECLARE bound_deployment TEXT:=okg.external_action_session_deployment();
BEGIN
  PERFORM okg.external_action_verifier_capability_status();
  PERFORM pg_advisory_xact_lock(hashtextextended('okg.external_action.deployment:'||bound_deployment,0));
  RETURN jsonb_build_object(
    'execution_epoch',COALESCE((SELECT max(epoch) FROM okg.external_action_execution_epoch_leases WHERE deployment_name=bound_deployment),0),
    'execution_epoch_lease_digest',(SELECT envelope_digest FROM okg.external_action_execution_epoch_leases WHERE deployment_name=bound_deployment ORDER BY epoch DESC,lease_sequence DESC LIMIT 1),
    'proposals',COALESCE((SELECT jsonb_agg(jsonb_build_object('key',p.proposal_id,'value',COALESCE(p.store_record,'{}'::jsonb)||(okg.external_action_logical_json_record(to_jsonb(p))-ARRAY['store_record','action_contract_record','submission_idempotency_record','created_at','updated_at','expired_at']::text[])) ORDER BY p.proposal_id) FROM okg.external_action_proposals p WHERE p.deployment_name=bound_deployment),'[]'::jsonb),
    'payload_versions',COALESCE((SELECT jsonb_agg(jsonb_build_object('bucket',v.proposal_id,'value',COALESCE(v.store_record,okg.external_action_logical_json_record(to_jsonb(v))-'store_record')||jsonb_build_object('version',v.payload_version,'difference',v.difference_field_codes,'canonical_payload',jsonb_build_object('__okg_bytes_b64',replace(encode(v.canonical_payload,'base64'),E'\n','')))) ORDER BY v.proposal_id,v.payload_version) FROM okg.external_action_payload_versions v WHERE v.deployment_name=bound_deployment),'[]'::jsonb),
    'auth_audits',COALESCE((SELECT jsonb_agg(jsonb_build_object('key',a.authentication_audit_id,'value',COALESCE(a.store_record,okg.external_action_logical_json_record(to_jsonb(a))-'store_record')) ORDER BY a.authentication_audit_id) FROM okg.external_action_auth_audits a WHERE a.deployment_name=bound_deployment),'[]'::jsonb),
    'decisions',COALESCE((SELECT jsonb_agg(jsonb_build_object('bucket',d.proposal_id,'value',jsonb_build_object('decision_id',d.decision_id,'proposal_id',d.proposal_id,'verdict',CASE WHEN d.verdict='approved' THEN 'approve' ELSE 'reject' END,'lifecycle_version',d.approved_lifecycle_version,'payload_version',d.approved_payload_version,'payload_hash',d.approved_payload_hash,'approver_principal',d.approver_principal,'approver_separation_subject',d.approver_separation_subject,'policy_digest',d.policy_digest,'execute_before',d.execute_before,'reason_code',d.reason_code,'decision_binding_hash',d.decision_binding_hash,'created_at',d.created_at,'authentication_audit_id',d.authentication_audit_id,'nomos_decision_id',d.nomos_decision_id)) ORDER BY d.proposal_id,d.decision_row_id) FROM okg.external_action_decisions d WHERE d.deployment_name=bound_deployment),'[]'::jsonb),
    'transition_idempotency',COALESCE((SELECT jsonb_agg(jsonb_build_object('key',jsonb_build_array(t.deployment_name,t.proposal_id,t.actor_separation_subject,t.idempotency_key_hash),'value',COALESCE(t.store_record,okg.external_action_logical_json_record(to_jsonb(t))-'store_record')) ORDER BY t.transition_idempotency_id) FROM okg.external_action_transition_idempotency t WHERE t.deployment_name=bound_deployment),'[]'::jsonb),
    'executions',COALESCE((SELECT jsonb_agg(jsonb_build_object('key',e.proposal_id,'value',COALESCE(e.store_record,'{}'::jsonb)||(okg.external_action_logical_json_record(to_jsonb(e))-ARRAY['store_record','canonical_payload']::text[])||jsonb_build_object('lifecycle_version',e.approved_lifecycle_version+1,'canonical_payload',jsonb_build_object('__okg_bytes_b64',replace(encode(e.canonical_payload,'base64'),E'\n','')),'decision_binding_hash',(SELECT d.decision_binding_hash FROM okg.external_action_decisions d WHERE d.deployment_name=e.deployment_name AND d.decision_id=e.decision_id),'claim_idempotency_key_hash',e.store_record->>'idempotency_key_hash','handoff_receipt_digest',e.okg_claim_handoff_receipt_digest)) ORDER BY e.execution_id) FROM okg.external_action_executions e WHERE e.deployment_name=bound_deployment),'[]'::jsonb),
    'outcomes',COALESCE((SELECT jsonb_agg(jsonb_build_object('key',o.execution_id,'value',COALESCE(o.store_record,'{}'::jsonb)||(okg.external_action_logical_json_record(to_jsonb(o))-'store_record')) ORDER BY o.outcome_id) FROM okg.external_action_outcomes o WHERE o.deployment_name=bound_deployment),'[]'::jsonb),
    'events',COALESCE((SELECT jsonb_agg(jsonb_build_object('bucket',e.effect_instance_key,'value',okg.external_action_logical_json_record(to_jsonb(e))-'store_record') ORDER BY e.effect_instance_key,e.sequence) FROM okg.external_action_events e WHERE e.deployment_name=bound_deployment),'[]'::jsonb),
    'attestations',COALESCE((SELECT jsonb_agg(jsonb_build_object('key',a.envelope_digest,'value',COALESCE(a.store_record,okg.external_action_logical_json_record(to_jsonb(a))-'store_record')) ORDER BY a.attestation_id) FROM okg.external_action_owner_unavailability_attestations a WHERE a.deployment_name=bound_deployment),'[]'::jsonb),
    'contracts',COALESCE((SELECT jsonb_agg(jsonb_build_object('key',p.action_contract_digest,'value',p.action_contract_record) ORDER BY p.action_contract_digest) FROM (SELECT DISTINCT ON (action_contract_digest) action_contract_digest,action_contract_record FROM okg.external_action_proposals WHERE deployment_name=bound_deployment AND action_contract_record IS NOT NULL ORDER BY action_contract_digest,proposal_id) p),'[]'::jsonb),
    'leases',COALESCE((SELECT jsonb_agg(jsonb_build_object('bucket',l.deployment_name,'value',COALESCE(l.store_record,okg.external_action_logical_json_record(to_jsonb(l))-'store_record')) ORDER BY l.epoch,l.lease_sequence) FROM okg.external_action_execution_epoch_leases l WHERE l.deployment_name=bound_deployment),'[]'::jsonb),
    'restore_fences',COALESCE((SELECT jsonb_agg(jsonb_build_object('key',f.deployment_name,'value',COALESCE(f.store_record,okg.external_action_logical_json_record(to_jsonb(f))-'store_record'))) FROM okg.external_action_restore_fences f WHERE f.deployment_name=bound_deployment),'[]'::jsonb),
    'restore_attestations',COALESCE((SELECT jsonb_agg(jsonb_build_object('key',a.restore_id,'value',COALESCE(a.store_record,okg.external_action_logical_json_record(to_jsonb(a))-'store_record')) ORDER BY a.attestation_row_id) FROM okg.external_action_restore_attestations a WHERE a.deployment_name=bound_deployment),'[]'::jsonb),
    'restore_sessions',COALESCE((SELECT jsonb_agg(jsonb_build_object('key',s.restore_id,'value',COALESCE(s.store_record,okg.external_action_logical_json_record(to_jsonb(s))-'store_record')) ORDER BY s.restore_session_row_id) FROM okg.external_action_restore_sessions s WHERE s.deployment_name=bound_deployment),'[]'::jsonb),
    'restore_completion_receipts',COALESCE((SELECT jsonb_agg(jsonb_build_object('key',r.restore_id,'value',COALESCE(r.store_record,okg.external_action_logical_json_record(to_jsonb(r))-'store_record')) ORDER BY r.completion_receipt_row_id) FROM okg.external_action_restore_completion_receipts r WHERE r.deployment_name=bound_deployment),'[]'::jsonb),
    'restore_checkpoint_staging',COALESCE((SELECT jsonb_agg(jsonb_build_object('key',jsonb_build_array(c.restore_id,c.batch_index),'value',COALESCE(c.store_record,okg.external_action_logical_json_record(to_jsonb(c))-'store_record')) ORDER BY c.checkpoint_staging_row_id) FROM okg.external_action_restore_checkpoint_staging c WHERE c.deployment_name=bound_deployment),'[]'::jsonb),
    'restore_checkpoint_entries',COALESCE((SELECT jsonb_agg(jsonb_build_object('key',jsonb_build_array(c.restore_id,c.batch_index,c.ordinal),'value',COALESCE(c.checkpoint_payload,jsonb_build_object('effect_instance_key',c.effect_instance_key,'checkpoint_envelope_digest',c.checkpoint_envelope_digest,'proof_digest',c.proof_digest))) ORDER BY c.checkpoint_entry_row_id) FROM okg.external_action_restore_checkpoint_entries c WHERE c.deployment_name=bound_deployment),'[]'::jsonb),
    'restore_finalize_idempotency',COALESCE((SELECT jsonb_agg(jsonb_build_object('key',r.restore_id,'value',jsonb_build_object('operator_separation_subject',r.operator_separation_subject,'idempotency_key_hash',r.idempotency_key_hash,'request_hash',r.request_hash,'response_body_jcs',jsonb_build_object('__okg_bytes_b64',encode(r.response_body_jcs,'base64')),'completion_receipt_envelope_digest',r.completion_receipt_envelope_digest)) ORDER BY r.finalize_idempotency_row_id) FROM okg.external_action_restore_finalize_idempotency r WHERE r.deployment_name=bound_deployment),'[]'::jsonb),
    'restore_recoveries',COALESCE((SELECT jsonb_agg(jsonb_build_object('key',jsonb_build_array(r.effect_instance_key,r.signed_worker_checkpoint_digest),'value',COALESCE(r.store_record,okg.external_action_logical_json_record(to_jsonb(r))-'store_record')) ORDER BY r.recovery_row_id) FROM okg.external_action_restore_recoveries r WHERE r.deployment_name=bound_deployment),'[]'::jsonb),
    'dpop_replay_nonces',COALESCE((SELECT jsonb_agg(jsonb_build_object('key',jsonb_build_array(n.issuer_digest,n.sender_key_thumbprint,n.jti_digest),'value',okg.external_action_logical_json_record(to_jsonb(n))) ORDER BY n.dpop_replay_nonce_id) FROM okg.external_action_dpop_replay_nonces n WHERE n.deployment_name=bound_deployment),'[]'::jsonb)
  );
END
$external_action_store_snapshot_v2$;

CREATE OR REPLACE FUNCTION okg.external_action_authorized_read(p_command JSONB)
RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $external_action_authorized_read$
DECLARE
  bound_deployment TEXT := okg.external_action_session_deployment();
  route_operation TEXT := p_command ->> 'operation';
  policy_action TEXT;
  proposal_row okg.external_action_proposals%ROWTYPE;
  payload_row okg.external_action_payload_versions%ROWTYPE;
  proposal_projection JSONB;
  proposals_projection JSONB;
  events_projection JSONB;
  recovery_projection JSONB;
  expiring RECORD;
BEGIN
  PERFORM okg.external_action_verifier_capability_status();
  PERFORM okg.external_action_require_command(
    p_command,
    ARRAY[
      'deployment','operation','request_hash','include_payload','resource','proposal_id',
      'effect_instance_key','state','limit','authentication_audit_id',
      'nomos_decision_id','policy_digest'
    ],
    ARRAY[
      'deployment','operation','request_hash','include_payload','resource',
      'authentication_audit_id','nomos_decision_id'
    ]
  );
  IF p_command ->> 'deployment' <> bound_deployment THEN
    RAISE EXCEPTION 'external action deployment is not bound to session_user'
      USING ERRCODE='28000';
  END IF;
  IF route_operation NOT IN (
       'read','read_payload','list','timeline','recovery_record'
     )
     OR jsonb_typeof(p_command -> 'include_payload') IS DISTINCT FROM 'boolean'
     OR (p_command ->> 'include_payload')::boolean IS DISTINCT FROM
          (route_operation = 'read_payload')
     OR p_command ->> 'resource' IS DISTINCT FROM (
          CASE route_operation
            WHEN 'recovery_record' THEN p_command ->> 'effect_instance_key'
            WHEN 'list' THEN bound_deployment
            ELSE p_command ->> 'proposal_id'
          END
        )
     OR p_command ->> 'request_hash' !~ '^sha256:[0-9a-f]{64}$' THEN
    RAISE EXCEPTION 'external action read request is invalid'
      USING ERRCODE='22023';
  END IF;
  IF route_operation IN ('read','read_payload','timeline')
     AND NULLIF(p_command ->> 'proposal_id','') IS NULL THEN
    RAISE EXCEPTION 'external action read proposal id is unavailable'
      USING ERRCODE='22023';
  END IF;
  IF route_operation = 'recovery_record'
     AND NULLIF(p_command ->> 'effect_instance_key','') IS NULL THEN
    RAISE EXCEPTION 'external action recovery key is unavailable'
      USING ERRCODE='22023';
  END IF;
  IF route_operation = 'list'
     AND p_command ->> 'state' IS NOT NULL
     AND p_command ->> 'state' NOT IN (
       'pending','edited','approved','rejected','expired','cancelled',
       'unavailable','ready','claimed','succeeded','failed','indeterminate'
     ) THEN
    RAISE EXCEPTION 'external action list state is invalid'
      USING ERRCODE='22023';
  END IF;
  IF p_command -> 'limit' <> 'null'::jsonb
     AND (jsonb_typeof(p_command -> 'limit') IS DISTINCT FROM 'number'
          OR (p_command ->> 'limit')::integer NOT BETWEEN 1 AND 1000) THEN
    RAISE EXCEPTION 'external action list limit is invalid'
      USING ERRCODE='22023';
  END IF;
  policy_action := CASE WHEN route_operation = 'read_payload'
    THEN 'external_action.read_payload' ELSE 'external_action.read' END;
  PERFORM pg_advisory_xact_lock(
    hashtextextended('okg.external_action.deployment:' || bound_deployment,0)
  );
  PERFORM okg.external_action_nomos_authority_snapshot(
    bound_deployment,p_command->>'authentication_audit_id',
    p_command->>'nomos_decision_id',policy_action,p_command->>'request_hash'
  );

  IF route_operation IN ('read','read_payload','timeline')
     AND EXISTS (
       SELECT 1
         FROM okg.external_action_proposals
        WHERE deployment_name=bound_deployment
          AND proposal_id=p_command->>'proposal_id'
     ) THEN
    PERFORM okg.external_action_materialize_expiry(p_command ->> 'proposal_id');
  ELSIF route_operation = 'list' THEN
    FOR expiring IN
      SELECT proposal_id
        FROM okg.external_action_proposals
       WHERE deployment_name=bound_deployment
         AND review_state IN ('pending','edited')
         AND effective_review_expires_at <= clock_timestamp()
       ORDER BY proposal_id
    LOOP
      PERFORM okg.external_action_materialize_expiry(expiring.proposal_id);
    END LOOP;
  END IF;

  IF route_operation IN ('read','read_payload') THEN
    SELECT * INTO proposal_row
      FROM okg.external_action_proposals
     WHERE deployment_name=bound_deployment
       AND proposal_id=p_command->>'proposal_id'
     FOR SHARE;
    IF proposal_row.proposal_id IS NULL THEN
      RETURN jsonb_build_object('proposal',NULL);
    END IF;
    proposal_projection := jsonb_build_object(
      'proposal_id',proposal_row.proposal_id,
      'deployment',proposal_row.deployment_name,
      'review_state',proposal_row.review_state,
      'execution_state',proposal_row.execution_state,
      'proposer_principal',proposal_row.store_record->>'proposer_principal',
      'proposer_separation_subject',proposal_row.proposer_separation_subject,
      'lifecycle_version',proposal_row.lifecycle_version,
      'payload_version',proposal_row.payload_version,
      'execution_epoch',proposal_row.execution_epoch,
      'effect_instance_key',proposal_row.effect_instance_key,
      'submission_content_hash',proposal_row.submission_content_hash,
      'original_payload_hash',proposal_row.original_payload_hash,
      'current_payload_hash',proposal_row.current_payload_hash,
      'proposal_request_hash',proposal_row.submission_request_hash,
      'automation_id',proposal_row.automation_id,
      'automation_version',proposal_row.automation_version,
      'run_id',proposal_row.run_id,
      'correlation_id',proposal_row.correlation_id,
      'action_type',proposal_row.action_type,
      'target_ref',proposal_row.target_ref,
      'risk_tier',proposal_row.risk_tier,
      'action_schema_digest',proposal_row.action_schema_digest,
      'action_contract_digest',proposal_row.action_contract_digest,
      'executor_contract_digest',proposal_row.executor_contract_digest,
      'payload_renderer_digest',proposal_row.payload_renderer_digest,
      'governed_tool_binding_digest',proposal_row.governed_tool_binding_digest,
      'provider_connection_contract_digest',proposal_row.provider_connection_contract_digest,
      'provider_capability_digest',proposal_row.provider_capability_digest,
      'provider_operation_id',proposal_row.provider_operation_id,
      'provider_operation_descriptor_digest',proposal_row.provider_operation_descriptor_digest,
      'requested_review_expires_at',to_char(
        proposal_row.requested_review_expires_at AT TIME ZONE 'UTC',
        'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
      ),
      'effective_review_expires_at',to_char(
        proposal_row.effective_review_expires_at AT TIME ZONE 'UTC',
        'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
      ),
      'summary_code',proposal_row.summary_code,
      'evidence_digests',proposal_row.evidence_digests,
      'decision_id',proposal_row.current_decision_id,
      'decision_binding_hash',proposal_row.current_decision_binding_hash,
      'execute_before',CASE WHEN proposal_row.execute_before IS NULL THEN NULL
        ELSE to_char(proposal_row.execute_before AT TIME ZONE 'UTC',
          'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') END,
      'policy_digest',proposal_row.store_record->>'policy_digest',
      'authentication_audit_id',proposal_row.store_record->>'authentication_audit_id',
      'nomos_decision_id',proposal_row.store_record->>'nomos_decision_id',
      'created_at',to_char(proposal_row.created_at AT TIME ZONE 'UTC',
        'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
      'updated_at',to_char(proposal_row.updated_at AT TIME ZONE 'UTC',
        'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
      'etag',format('"lifecycle:%s;payload-version:%s;payload:%s"',
        proposal_row.lifecycle_version,proposal_row.payload_version,
        proposal_row.current_payload_hash)
    );
    IF route_operation = 'read_payload' THEN
      SELECT * INTO payload_row
        FROM okg.external_action_payload_versions
       WHERE deployment_name=bound_deployment
         AND proposal_id=proposal_row.proposal_id
         AND payload_version=proposal_row.payload_version
         AND payload_hash=proposal_row.current_payload_hash
       FOR SHARE;
      IF payload_row.payload_version_id IS NULL THEN
        RAISE EXCEPTION 'external action payload is unavailable'
          USING ERRCODE='40001';
      END IF;
      proposal_projection := proposal_projection || jsonb_build_object(
        'canonical_payload',jsonb_build_object(
          '__okg_bytes_b64',replace(encode(payload_row.canonical_payload,'base64'),E'\n','')
        )
      );
    END IF;
    RETURN jsonb_build_object('proposal',proposal_projection);
  END IF;

  IF route_operation = 'list' THEN
    SELECT COALESCE(jsonb_agg(projection ORDER BY projection->>'proposal_id'),'[]'::jsonb)
      INTO proposals_projection
      FROM (
        SELECT jsonb_build_object(
          'proposal_id',p.proposal_id,'deployment',p.deployment_name,
          'review_state',p.review_state,'execution_state',p.execution_state,
          'proposer_principal',p.store_record->>'proposer_principal',
          'proposer_separation_subject',p.proposer_separation_subject,
          'lifecycle_version',p.lifecycle_version,'payload_version',p.payload_version,
          'execution_epoch',p.execution_epoch,'effect_instance_key',p.effect_instance_key,
          'submission_content_hash',p.submission_content_hash,
          'original_payload_hash',p.original_payload_hash,
          'current_payload_hash',p.current_payload_hash,
          'proposal_request_hash',p.submission_request_hash,
          'action_type',p.action_type,'target_ref',p.target_ref,'risk_tier',p.risk_tier,
          'action_schema_digest',p.action_schema_digest,
          'action_contract_digest',p.action_contract_digest,
          'executor_contract_digest',p.executor_contract_digest,
          'payload_renderer_digest',p.payload_renderer_digest,
          'governed_tool_binding_digest',p.governed_tool_binding_digest,
          'provider_connection_contract_digest',p.provider_connection_contract_digest,
          'provider_capability_digest',p.provider_capability_digest,
          'provider_operation_id',p.provider_operation_id,
          'provider_operation_descriptor_digest',p.provider_operation_descriptor_digest,
          'requested_review_expires_at',to_char(p.requested_review_expires_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
          'effective_review_expires_at',to_char(p.effective_review_expires_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
          'summary_code',p.summary_code,'evidence_digests',p.evidence_digests,
          'decision_id',p.current_decision_id,
          'decision_binding_hash',p.current_decision_binding_hash,
          'execute_before',CASE WHEN p.execute_before IS NULL THEN NULL ELSE to_char(p.execute_before AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"') END,
          'policy_digest',p.store_record->>'policy_digest',
          'authentication_audit_id',p.store_record->>'authentication_audit_id',
          'nomos_decision_id',p.store_record->>'nomos_decision_id',
          'created_at',to_char(p.created_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
          'updated_at',to_char(p.updated_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
          'etag',format('"lifecycle:%s;payload-version:%s;payload:%s"',p.lifecycle_version,p.payload_version,p.current_payload_hash)
        ) AS projection
          FROM okg.external_action_proposals p
         WHERE p.deployment_name=bound_deployment
           AND (p_command->>'state' IS NULL
                OR p.review_state=p_command->>'state'
                OR p.execution_state=p_command->>'state')
         ORDER BY p.proposal_id
         LIMIT COALESCE((p_command->>'limit')::integer,1000)
      ) AS rows;
    RETURN jsonb_build_object('proposals',proposals_projection);
  END IF;

  IF route_operation = 'timeline' THEN
    SELECT * INTO proposal_row
      FROM okg.external_action_proposals
     WHERE deployment_name=bound_deployment
       AND proposal_id=p_command->>'proposal_id'
     FOR SHARE;
    IF proposal_row.proposal_id IS NULL THEN
      RETURN jsonb_build_object('events',NULL);
    END IF;
    SELECT COALESCE(jsonb_agg(
             COALESCE(e.store_record->'event_projection',okg.external_action_logical_json_record(to_jsonb(e))-ARRAY['event_id','deployment_name','store_record']::text[])
             || jsonb_build_object('event_hash',e.event_hash)
             ORDER BY e.sequence
           ),'[]'::jsonb)
      INTO events_projection
      FROM okg.external_action_events e
     WHERE e.deployment_name=bound_deployment
       AND e.effect_instance_key=proposal_row.effect_instance_key;
    RETURN jsonb_build_object('events',events_projection);
  END IF;

  SELECT okg.external_action_logical_json_record(to_jsonb(r))-ARRAY['recovery_row_id','store_record']::text[]
    INTO recovery_projection
    FROM okg.external_action_restore_recoveries r
   WHERE r.deployment_name=bound_deployment
     AND r.effect_instance_key=p_command->>'effect_instance_key'
   ORDER BY r.new_epoch DESC,r.recorded_at DESC,r.recovery_row_id DESC
   LIMIT 1;
  RETURN jsonb_build_object('recovery_record',recovery_projection);
END
$external_action_authorized_read$;
REVOKE ALL ON FUNCTION okg.external_action_authorized_read(JSONB) FROM PUBLIC;

CREATE OR REPLACE FUNCTION okg.external_action_store_startup(p_state JSONB)
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, okg AS $external_action_store_startup_v2$
DECLARE
  bound_deployment TEXT:=okg.external_action_session_deployment();
  fence_row okg.external_action_restore_fences%ROWTYPE;
  lease_row okg.external_action_execution_epoch_leases%ROWTYPE;
  lease_count BIGINT;
  derived_mode TEXT;
  derived_quarantine TEXT;
  epoch_advanced BOOLEAN;
BEGIN
  PERFORM okg.external_action_verifier_capability_status();
  -- Mode and quarantine are database decisions.  Keep the legacy fields in
  -- the allow-list for adapter compatibility, but never read them.
  PERFORM okg.external_action_require_command(p_state,ARRAY['deployment','highest_accepted_epoch','lease_digest','service_mode','quarantine_state','lease_chain_digest','verification_config_digest','verification_login_binding_digest'],ARRAY['deployment','highest_accepted_epoch','lease_digest','lease_chain_digest','verification_config_digest','verification_login_binding_digest']);
  IF p_state->>'deployment'<>bound_deployment THEN RAISE EXCEPTION 'external action deployment is not bound to session_user' USING ERRCODE='28000'; END IF;
  IF (p_state->>'highest_accepted_epoch')::bigint < 0 THEN
    RAISE EXCEPTION 'external action startup epoch is invalid' USING ERRCODE='22023';
  END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended('okg.external_action.deployment:'||bound_deployment,0));
  SELECT * INTO fence_row FROM okg.external_action_restore_fences WHERE deployment_name=bound_deployment FOR UPDATE;
  SELECT * INTO lease_row
    FROM okg.external_action_execution_epoch_leases
   WHERE deployment_name=bound_deployment
   ORDER BY epoch DESC, lease_sequence DESC
   LIMIT 1;
  SELECT count(*) INTO lease_count
    FROM okg.external_action_execution_epoch_leases
   WHERE deployment_name=bound_deployment;
  IF lease_row.lease_row_id IS NULL
     OR lease_row.epoch<>(p_state->>'highest_accepted_epoch')::bigint
     OR lease_row.envelope_digest<>p_state->>'lease_digest'
     OR lease_row.lease_chain_digest IS DISTINCT FROM p_state->>'lease_chain_digest'
     OR lease_row.verification_config_digest<>p_state->>'verification_config_digest'
     OR lease_row.verification_login_binding_digest<>p_state->>'verification_login_binding_digest' THEN
    RAISE EXCEPTION 'startup fence does not match the installed verified lease'
      USING ERRCODE='40001';
  END IF;
  IF fence_row.deployment_name IS NULL THEN
    -- A lone genesis lease is safe on first use even when its epoch is
    -- nonzero.  Any retained suffix requires restore-only quarantine.
    derived_mode := CASE WHEN lease_count=1 AND lease_row.lease_sequence=1
                              AND lease_row.previous_lease_digest IS NULL
                              AND lease_row.previous_epoch_digest IS NULL
                         THEN 'normal' ELSE 'restore_only' END;
    derived_quarantine := CASE WHEN derived_mode='normal' THEN 'clear' ELSE 'blocked' END;
    INSERT INTO okg.external_action_restore_fences (deployment_name,highest_accepted_epoch,lease_digest,quarantine_state,service_mode,lease_chain_digest,verification_config_digest,verification_login_binding_digest,accepted_at,mode_changed_at,store_record)
    VALUES (bound_deployment,lease_row.epoch,lease_row.envelope_digest,derived_quarantine,derived_mode,lease_row.lease_chain_digest,lease_row.verification_config_digest,lease_row.verification_login_binding_digest,clock_timestamp(),clock_timestamp(),p_state);
  ELSE
    IF lease_row.epoch < fence_row.highest_accepted_epoch THEN
      RAISE EXCEPTION 'startup lease epoch is stale' USING ERRCODE='40001';
    END IF;
    epoch_advanced := lease_row.epoch > fence_row.highest_accepted_epoch;
    -- Replaying the exact installed lease is a true no-op.  In particular,
    -- do not refresh accepted_at or store_record: those columns are the
    -- durable evidence of the first acceptance and must remain stable when
    -- a restarted process re-presents the same verified lease.
    IF NOT epoch_advanced
       AND fence_row.lease_digest IS NOT DISTINCT FROM lease_row.envelope_digest
       AND fence_row.lease_chain_digest IS NOT DISTINCT FROM lease_row.lease_chain_digest
       AND fence_row.verification_config_digest IS NOT DISTINCT FROM lease_row.verification_config_digest
       AND fence_row.verification_login_binding_digest IS NOT DISTINCT FROM lease_row.verification_login_binding_digest THEN
      RETURN jsonb_build_object('replay',TRUE,'accepted',TRUE,'deployment',bound_deployment,
        'service_mode',fence_row.service_mode,'quarantine_state',fence_row.quarantine_state,
        'highest_accepted_epoch',fence_row.highest_accepted_epoch);
    END IF;
    derived_mode := CASE WHEN epoch_advanced THEN 'restore_only' ELSE fence_row.service_mode END;
    derived_quarantine := CASE WHEN epoch_advanced THEN 'blocked' ELSE fence_row.quarantine_state END;
    UPDATE okg.external_action_restore_fences
       SET highest_accepted_epoch=lease_row.epoch,
           lease_digest=lease_row.envelope_digest,
           quarantine_state=derived_quarantine,
           service_mode=derived_mode,
           lease_chain_digest=lease_row.lease_chain_digest,
           verification_config_digest=lease_row.verification_config_digest,
           verification_login_binding_digest=lease_row.verification_login_binding_digest,
           accepted_at=clock_timestamp(),
           mode_changed_at=CASE WHEN epoch_advanced THEN clock_timestamp() ELSE mode_changed_at END,
           store_record=p_state
     WHERE deployment_name=bound_deployment;
  END IF;
  RETURN jsonb_build_object('replay',fence_row.deployment_name IS NOT NULL,'accepted',TRUE,'deployment',bound_deployment,'service_mode',derived_mode,'quarantine_state',derived_quarantine,'highest_accepted_epoch',lease_row.epoch);
END
$external_action_store_startup_v2$;
CREATE OR REPLACE FUNCTION okg.external_action_store_resolve_principal(p_issuer TEXT,p_subject TEXT)
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, okg AS $external_action_store_resolve_principal_v2$
DECLARE bound_deployment TEXT:=okg.external_action_session_deployment(); matched_count INTEGER; result JSONB;
BEGIN
  PERFORM okg.external_action_verifier_capability_status();
  SELECT count(*)::integer INTO matched_count FROM okg.nomos_identity_providers p JOIN okg.nomos_subject_bindings b ON b.deployment_name=p.deployment_name AND b.provider_id=p.provider_id AND b.issuer=p.issuer WHERE p.deployment_name=bound_deployment AND p.issuer=p_issuer AND p.status='active' AND b.status='active' AND b.claim_path=p.subject_claim AND b.claim_value=p_subject AND (b.effective_from IS NULL OR b.effective_from<=clock_timestamp()) AND (b.effective_until IS NULL OR clock_timestamp()<b.effective_until);
  IF matched_count<>1 THEN RAISE EXCEPTION 'deployment-pinned principal mapping is unavailable' USING ERRCODE='28000'; END IF;
  SELECT jsonb_build_object(
    'principal',b.subject,'separation_subject',b.subject,'provider_id',p.provider_id,
    'subject_binding_id',b.binding_id,'issuer',p.issuer,'deployment',bound_deployment,
    'provider_status',p.status,'provider_updated_at',p.updated_at,
    'provider_credential_generation_digest',COALESCE(
      p.metadata->>'credential_generation_digest',
      p.metadata->>'provider_credential_generation_digest',
      p.metadata->>'credential_generation'),
    'provider_certificate_thumbprint',COALESCE(
      p.metadata->>'certificate_thumbprint',p.metadata->>'sender_key_thumbprint'),
    'provider_certificate_not_before',p.metadata->>'certificate_not_before',
    'provider_certificate_not_after',p.metadata->>'certificate_not_after',
    'subject',b.subject,'binding_status',b.status,'binding_updated_at',b.updated_at,
    'binding_credential_generation_digest',COALESCE(
      b.metadata->>'credential_generation_digest',
      b.metadata->>'binding_credential_generation_digest',
      b.metadata->>'credential_generation'),
    'binding_certificate_thumbprint',COALESCE(
      b.metadata->>'certificate_thumbprint',b.metadata->>'sender_key_thumbprint'),
    'binding_certificate_not_before',b.metadata->>'certificate_not_before',
    'binding_certificate_not_after',b.metadata->>'certificate_not_after',
    'certificate_not_before',COALESCE(
      b.metadata->>'certificate_not_before',p.metadata->>'certificate_not_before'),
    'certificate_not_after',COALESCE(
      b.metadata->>'certificate_not_after',p.metadata->>'certificate_not_after')
  ) INTO result FROM okg.nomos_identity_providers p JOIN okg.nomos_subject_bindings b ON b.deployment_name=p.deployment_name AND b.provider_id=p.provider_id AND b.issuer=p.issuer WHERE p.deployment_name=bound_deployment AND p.issuer=p_issuer AND p.status='active' AND b.status='active' AND b.claim_path=p.subject_claim AND b.claim_value=p_subject AND (b.effective_from IS NULL OR b.effective_from<=clock_timestamp()) AND (b.effective_until IS NULL OR clock_timestamp()<b.effective_until);
  RETURN result;
END
$external_action_store_resolve_principal_v2$;

-- Persist the verified startup lease through one fixed, closed command.  This
-- is the pre-bind suffix path: no raw table grant or relation name is exposed.
CREATE OR REPLACE FUNCTION okg.external_action_store_insert_lease(p_command JSONB)
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $external_action_store_insert_lease$
DECLARE
  bound_deployment TEXT := okg.external_action_session_deployment();
  lease_row okg.external_action_execution_epoch_leases%ROWTYPE;
  prior_row okg.external_action_execution_epoch_leases%ROWTYPE;
  prior_epoch_row okg.external_action_execution_epoch_leases%ROWTYPE;
  inserted_count INTEGER;
  existing_lease_count BIGINT;
BEGIN
  PERFORM okg.external_action_verifier_capability_status();
  PERFORM okg.external_action_require_command(
    p_command,
    ARRAY['deployment','epoch','lease_sequence','previous_lease_digest',
      'previous_epoch_digest','issued_at','expires_at',
      'signer_separation_subject','nonce','protected_b64','payload_b64',
      'signature_b64','signer_key_id_digest','envelope_digest',
      'verification_source','verification_config_digest',
      'verification_login_binding_digest','lease_chain_digest',
      'authentication_audit_id','nomos_decision_id'],
    ARRAY['deployment','epoch','lease_sequence','issued_at','expires_at',
      'signer_separation_subject','nonce','protected_b64','payload_b64',
      'signature_b64','envelope_digest','verification_source',
      'verification_config_digest','verification_login_binding_digest']
  );
  IF p_command->>'deployment' <> bound_deployment THEN
    RAISE EXCEPTION 'external action deployment is not bound to session_user'
      USING ERRCODE='28000';
  END IF;
  IF NOT (p_command ? 'previous_lease_digest')
     OR NOT (p_command ? 'previous_epoch_digest') THEN
    RAISE EXCEPTION 'external action lease predecessor fields are required'
      USING ERRCODE='22023';
  END IF;
  IF p_command->>'signer_separation_subject' = ''
     OR p_command->>'nonce' = '' OR p_command->>'protected_b64' = ''
     OR p_command->>'payload_b64' = '' OR p_command->>'signature_b64' = ''
     OR p_command->>'signer_key_id_digest' = ''
     OR p_command->>'envelope_digest' = ''
     OR p_command->>'verification_config_digest' = ''
     OR p_command->>'verification_login_binding_digest' = '' THEN
    RAISE EXCEPTION 'external action lease contains a blank field'
      USING ERRCODE='22023';
  END IF;
  IF p_command->>'verification_source' NOT IN
       ('signed_startup_gate','strict_authenticated_api') THEN
    RAISE EXCEPTION 'external action lease verification source is not allowed'
      USING ERRCODE='22023';
  END IF;
  IF p_command->>'verification_source' = 'signed_startup_gate'
     AND (p_command->>'lease_chain_digest' IS NULL
          OR p_command->>'authentication_audit_id' IS NOT NULL
          OR p_command->>'nomos_decision_id' IS NOT NULL) THEN
    RAISE EXCEPTION 'startup lease authority fields are inconsistent'
      USING ERRCODE='22023';
  ELSIF p_command->>'verification_source' = 'strict_authenticated_api'
        AND (p_command->>'lease_chain_digest' IS NOT NULL
             OR p_command->>'authentication_audit_id' IS NULL
             OR p_command->>'nomos_decision_id' IS NULL) THEN
    RAISE EXCEPTION 'authenticated lease authority fields are inconsistent'
      USING ERRCODE='22023';
  END IF;
  IF (p_command->>'issued_at')::timestamptz >=
       (p_command->>'expires_at')::timestamptz
     OR (p_command->>'epoch')::bigint < 0
     OR (p_command->>'lease_sequence')::bigint < 1 THEN
    RAISE EXCEPTION 'external action lease interval or sequence is invalid'
      USING ERRCODE='22023';
  END IF;
  PERFORM pg_advisory_xact_lock(
    hashtextextended('okg.external_action.deployment:'||bound_deployment,0)
  );
  SELECT count(*) INTO existing_lease_count
    FROM okg.external_action_execution_epoch_leases
   WHERE deployment_name=bound_deployment;
  IF p_command->>'verification_source' = 'strict_authenticated_api' THEN
    PERFORM okg.external_action_nomos_authority_snapshot(
      bound_deployment,p_command->>'authentication_audit_id',
      p_command->>'nomos_decision_id','external_action.execute',
      p_command->>'envelope_digest');
  END IF;

  -- Idempotency is checked before chain guards.  Retries return the exact
  -- stored evidence and do not re-evaluate a historical lease boundary.
  SELECT * INTO lease_row
    FROM okg.external_action_execution_epoch_leases
   WHERE deployment_name=bound_deployment
     AND envelope_digest=p_command->>'envelope_digest'
   FOR UPDATE;
  IF lease_row.lease_row_id IS NOT NULL THEN
    IF lease_row.epoch <> (p_command->>'epoch')::bigint
       OR lease_row.lease_sequence <> (p_command->>'lease_sequence')::bigint
       OR lease_row.previous_lease_digest IS DISTINCT FROM p_command->>'previous_lease_digest'
       OR lease_row.previous_epoch_digest IS DISTINCT FROM p_command->>'previous_epoch_digest'
       OR lease_row.issued_at <> (p_command->>'issued_at')::timestamptz
       OR lease_row.expires_at <> (p_command->>'expires_at')::timestamptz
       OR lease_row.signer_separation_subject <> p_command->>'signer_separation_subject'
       OR lease_row.nonce <> p_command->>'nonce'
       OR lease_row.protected_b64 <> p_command->>'protected_b64'
       OR lease_row.payload_b64 <> p_command->>'payload_b64'
       OR lease_row.signature_b64 <> p_command->>'signature_b64'
       OR lease_row.signer_key_id_digest IS DISTINCT FROM p_command->>'signer_key_id_digest'
       OR lease_row.verification_source <> p_command->>'verification_source'
       OR lease_row.verification_config_digest <> p_command->>'verification_config_digest'
       OR lease_row.verification_login_binding_digest <> p_command->>'verification_login_binding_digest'
       OR lease_row.lease_chain_digest IS DISTINCT FROM p_command->>'lease_chain_digest'
       OR lease_row.authentication_audit_id IS DISTINCT FROM p_command->>'authentication_audit_id'
       OR lease_row.nomos_decision_id IS DISTINCT FROM p_command->>'nomos_decision_id' THEN
      RAISE EXCEPTION 'external action lease id was reused with a different request'
        USING ERRCODE='40001';
    END IF;
    RETURN jsonb_build_object('replay',TRUE,'deployment',bound_deployment,
      'epoch',lease_row.epoch,'lease_sequence',lease_row.lease_sequence,
      'envelope_digest',lease_row.envelope_digest,
      'lease_chain_digest',lease_row.lease_chain_digest);
  END IF;
  SELECT * INTO prior_row
    FROM okg.external_action_execution_epoch_leases
   WHERE deployment_name=bound_deployment AND epoch=(p_command->>'epoch')::bigint
     AND lease_sequence=(p_command->>'lease_sequence')::bigint
   FOR UPDATE;
  IF prior_row.lease_row_id IS NOT NULL THEN
    RAISE EXCEPTION 'external action lease sequence was reused with a different envelope'
      USING ERRCODE='40001';
  END IF;
  IF (p_command->>'lease_sequence')::bigint = 1 THEN
    IF existing_lease_count = 0 THEN
      IF p_command->>'previous_lease_digest' IS NOT NULL
         OR p_command->>'previous_epoch_digest' IS NOT NULL THEN
        RAISE EXCEPTION 'first external action lease has a predecessor'
          USING ERRCODE='40001';
      END IF;
    ELSE
      SELECT * INTO prior_epoch_row
        FROM okg.external_action_execution_epoch_leases
       WHERE deployment_name=bound_deployment AND epoch < (p_command->>'epoch')::bigint
       ORDER BY epoch DESC, lease_sequence DESC LIMIT 1 FOR UPDATE;
      IF prior_epoch_row.lease_row_id IS NULL
         OR p_command->>'previous_lease_digest' IS NOT NULL
         OR p_command->>'previous_epoch_digest' <> prior_epoch_row.envelope_digest THEN
        RAISE EXCEPTION 'external action lease epoch predecessor is invalid'
          USING ERRCODE='40001';
      END IF;
    END IF;
  ELSE
    SELECT * INTO prior_row
      FROM okg.external_action_execution_epoch_leases
     WHERE deployment_name=bound_deployment AND epoch=(p_command->>'epoch')::bigint
       AND lease_sequence=(p_command->>'lease_sequence')::bigint-1
     FOR UPDATE;
    IF prior_row.lease_row_id IS NULL
       OR p_command->>'previous_lease_digest' <> prior_row.envelope_digest
       OR p_command->>'previous_epoch_digest' IS DISTINCT FROM prior_row.previous_epoch_digest THEN
      RAISE EXCEPTION 'external action lease chain predecessor is invalid'
        USING ERRCODE='40001';
    END IF;
  END IF;
  INSERT INTO okg.external_action_execution_epoch_leases (
    deployment_name,epoch,lease_sequence,previous_lease_digest,previous_epoch_digest,
    issued_at,expires_at,signer_separation_subject,nonce,protected_b64,payload_b64,
    signature_b64,signer_key_id_digest,envelope_digest,verified_at,verification_source,
    verification_config_digest,verification_login_binding_digest,lease_chain_digest,
    authentication_audit_id,nomos_decision_id,store_record
  ) VALUES (
    bound_deployment,(p_command->>'epoch')::bigint,
    (p_command->>'lease_sequence')::bigint,p_command->>'previous_lease_digest',
    p_command->>'previous_epoch_digest',(p_command->>'issued_at')::timestamptz,
    (p_command->>'expires_at')::timestamptz,p_command->>'signer_separation_subject',
    p_command->>'nonce',p_command->>'protected_b64',p_command->>'payload_b64',
    p_command->>'signature_b64',p_command->>'signer_key_id_digest',p_command->>'envelope_digest',
    clock_timestamp(),p_command->>'verification_source',
    p_command->>'verification_config_digest',p_command->>'verification_login_binding_digest',
    p_command->>'lease_chain_digest',p_command->>'authentication_audit_id',
    p_command->>'nomos_decision_id',p_command
  ) ON CONFLICT (deployment_name,envelope_digest) DO NOTHING;
  GET DIAGNOSTICS inserted_count=ROW_COUNT;
  SELECT * INTO lease_row
    FROM okg.external_action_execution_epoch_leases
   WHERE deployment_name=bound_deployment AND envelope_digest=p_command->>'envelope_digest'
   FOR UPDATE;
  IF lease_row.lease_row_id IS NULL THEN
    RAISE EXCEPTION 'external action lease was not stored' USING ERRCODE='40001';
  END IF;
  RETURN jsonb_build_object('replay',inserted_count<>1,'deployment',bound_deployment,
    'epoch',lease_row.epoch,'lease_sequence',lease_row.lease_sequence,
    'envelope_digest',lease_row.envelope_digest,
    'lease_chain_digest',lease_row.lease_chain_digest);
END
$external_action_store_insert_lease$;

-- Read paths call this fixed transition so expiry is decided by the database
-- clock and the resulting event is durable and at-most-once.
CREATE OR REPLACE FUNCTION okg.external_action_materialize_expiry(p_proposal_id TEXT)
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $external_action_materialize_expiry$
DECLARE
  bound_deployment TEXT := okg.external_action_session_deployment();
  proposal_row okg.external_action_proposals%ROWTYPE;
  prior_review_state TEXT;
  prior_execution_state TEXT;
  event_type_value TEXT;
  deadline_kind_value TEXT;
  deadline_value TIMESTAMPTZ;
  observed_at TIMESTAMPTZ := clock_timestamp();
  event_sequence BIGINT;
  previous_event_hash_value TEXT;
  event_hash_value TEXT;
  event_projection JSONB;
BEGIN
  PERFORM okg.external_action_verifier_capability_status();
  IF p_proposal_id IS NULL OR btrim(p_proposal_id)='' THEN
    RAISE EXCEPTION 'external action proposal id is required' USING ERRCODE='22023';
  END IF;
  PERFORM pg_advisory_xact_lock(
    hashtextextended('okg.external_action.deployment:'||bound_deployment,0)
  );
  SELECT * INTO proposal_row FROM okg.external_action_proposals
   WHERE deployment_name=bound_deployment AND proposal_id=p_proposal_id FOR UPDATE;
  IF proposal_row.proposal_id IS NULL THEN
    RAISE EXCEPTION 'external action proposal was not found' USING ERRCODE='P0002';
  END IF;
  IF proposal_row.review_state='expired' THEN
    RETURN jsonb_build_object('replay',TRUE,'expired',TRUE,'deployment',bound_deployment,
      'proposal_id',proposal_row.proposal_id,'proposal',okg.external_action_logical_json_record(to_jsonb(proposal_row))-'store_record');
  END IF;
  IF proposal_row.review_state IN ('pending','edited')
     AND observed_at >= proposal_row.effective_review_expires_at THEN
    prior_review_state:=proposal_row.review_state;
    prior_execution_state:=proposal_row.execution_state;
    event_type_value:='review_expired'; deadline_kind_value:='review_expires_at';
    deadline_value:=proposal_row.effective_review_expires_at;
  ELSIF proposal_row.review_state='approved'
        AND proposal_row.execution_state='ready'
        AND proposal_row.execute_before IS NOT NULL
        AND observed_at >= proposal_row.execute_before THEN
    prior_review_state:=proposal_row.review_state;
    prior_execution_state:=proposal_row.execution_state;
    event_type_value:='approval_expired'; deadline_kind_value:='execute_before';
    deadline_value:=proposal_row.execute_before;
  ELSE
    RETURN jsonb_build_object('replay',FALSE,'expired',FALSE,'deployment',bound_deployment,
      'proposal_id',proposal_row.proposal_id,'proposal',okg.external_action_logical_json_record(to_jsonb(proposal_row))-'store_record');
  END IF;
  UPDATE okg.external_action_proposals
     SET review_state='expired',execution_state='unavailable',
         lifecycle_version=lifecycle_version+1,updated_at=observed_at,expired_at=observed_at,
         store_record=COALESCE(store_record,'{}'::jsonb)||jsonb_build_object(
           'review_state','expired','execution_state','unavailable',
           'lifecycle_version',lifecycle_version+1,'updated_at',observed_at,
           'expired_at',observed_at)
   WHERE deployment_name=bound_deployment AND proposal_id=proposal_row.proposal_id
  RETURNING * INTO proposal_row;
  SELECT COALESCE(max(sequence),0)+1,
         (array_agg(event_hash ORDER BY sequence))[array_length(array_agg(event_hash ORDER BY sequence),1)]
    INTO event_sequence,previous_event_hash_value
    FROM okg.external_action_events
   WHERE deployment_name=bound_deployment AND effect_instance_key=proposal_row.effect_instance_key;
  event_projection := jsonb_build_object(
    'proposal_id', proposal_row.proposal_id,
    'execution_epoch', proposal_row.execution_epoch,
    'effect_instance_key', proposal_row.effect_instance_key,
    'sequence', event_sequence,
    'lifecycle_version', proposal_row.lifecycle_version,
    'event_type', event_type_value,
    'actor_kind', 'system_clock',
    'actor_principal', NULL::TEXT,
    'actor_separation_subject', NULL::TEXT,
    'authentication_audit_ref', NULL::TEXT,
    'nomos_decision_ref', NULL::TEXT,
    'deadline_kind', deadline_kind_value,
    'policy_digest', NULL::TEXT,
    'before_review_state', prior_review_state,
    'after_review_state', 'expired',
    'before_execution_state', prior_execution_state,
    'after_execution_state', 'unavailable',
    'payload_version', proposal_row.payload_version,
    'payload_hash', proposal_row.current_payload_hash,
    'decision_id', proposal_row.current_decision_id,
    'decision_binding_hash', proposal_row.current_decision_binding_hash,
    'execution_id', NULL::TEXT,
    'execution_binding_hash', NULL::TEXT,
    'transition_reason_code', NULL::TEXT,
    'outcome', NULL::TEXT,
    'outcome_reason_code', NULL::TEXT,
    'outcome_request_hash', NULL::TEXT,
    'restore_recovery_digest', NULL::TEXT,
    'effective_at', to_char(deadline_value AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
    'recorded_at', to_char(observed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
    'previous_event_hash', previous_event_hash_value
  );
  event_hash_value:=okg.external_action_hash('okg-external-action-event-v1',event_projection);
  INSERT INTO okg.external_action_events (
    deployment_name,proposal_id,execution_epoch,effect_instance_key,sequence,lifecycle_version,
    event_type,actor_kind,actor_principal,actor_separation_subject,
    authentication_audit_ref,nomos_decision_ref,deadline_kind,policy_digest,
    before_review_state,after_review_state,before_execution_state,after_execution_state,
    payload_version,payload_hash,decision_id,decision_binding_hash,execution_id,
    execution_binding_hash,transition_reason_code,outcome,outcome_reason_code,
    outcome_request_hash,restore_recovery_digest,effective_at,recorded_at,
    previous_event_hash,event_hash,store_record
  ) VALUES (
    bound_deployment,proposal_row.proposal_id,proposal_row.execution_epoch,
    proposal_row.effect_instance_key,event_sequence,proposal_row.lifecycle_version,event_type_value,
    'system_clock',NULL,NULL,NULL,NULL,deadline_kind_value,NULL,prior_review_state,'expired',
    prior_execution_state,'unavailable',proposal_row.payload_version,
    proposal_row.current_payload_hash,proposal_row.current_decision_id,
    proposal_row.current_decision_binding_hash,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
    deadline_value,observed_at,previous_event_hash_value,event_hash_value,
    jsonb_build_object('event_projection', event_projection)
  ) ON CONFLICT (deployment_name,effect_instance_key,event_hash) DO NOTHING;
  RETURN jsonb_build_object('replay',FALSE,'expired',TRUE,'deployment',bound_deployment,
    'proposal_id',proposal_row.proposal_id,'event_hash',event_hash_value,
    'proposal',okg.external_action_logical_json_record(to_jsonb(proposal_row))-'store_record');
END
$external_action_materialize_expiry$;

DO $external_action_semantic_function_grants$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname='okg_external_action_api') THEN
    GRANT EXECUTE ON FUNCTION okg.external_action_session_deployment() TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_assert_deployment(TEXT) TO okg_external_action_api;
    REVOKE ALL ON FUNCTION okg.external_action_startup_login_binding() FROM PUBLIC;
    REVOKE ALL ON FUNCTION okg.external_action_record_auth_audit(JSONB) FROM PUBLIC;
    REVOKE ALL ON FUNCTION okg.external_action_nomos_authority_snapshot(TEXT,TEXT,TEXT,TEXT,TEXT) FROM PUBLIC;
    REVOKE ALL ON FUNCTION okg.external_action_nomos_persist_decision(JSONB) FROM PUBLIC;
    REVOKE ALL ON FUNCTION okg.external_action_submit(JSONB) FROM PUBLIC;
    REVOKE ALL ON FUNCTION okg.external_action_review_transition(JSONB) FROM PUBLIC;
    REVOKE ALL ON FUNCTION okg.external_action_claim(JSONB) FROM PUBLIC;
    REVOKE ALL ON FUNCTION okg.external_action_prepare_claim(JSONB) FROM PUBLIC;
    REVOKE ALL ON FUNCTION okg.external_action_authorized_read(JSONB) FROM PUBLIC;
    REVOKE ALL ON FUNCTION okg.external_action_report_outcome(JSONB) FROM PUBLIC;
    REVOKE ALL ON FUNCTION okg.external_action_owner_unavailability(JSONB) FROM PUBLIC;
    REVOKE ALL ON FUNCTION okg.external_action_restore_create(JSONB) FROM PUBLIC;
    REVOKE ALL ON FUNCTION okg.external_action_restore_batch(JSONB) FROM PUBLIC;
    REVOKE ALL ON FUNCTION okg.external_action_restore_finalize(JSONB) FROM PUBLIC;
    REVOKE ALL ON FUNCTION okg.external_action_store_insert_lease(JSONB) FROM PUBLIC;
    REVOKE ALL ON FUNCTION okg.external_action_materialize_expiry(TEXT) FROM PUBLIC;
    GRANT EXECUTE ON FUNCTION okg.external_action_record_auth_audit(JSONB) TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_nomos_authority_snapshot(TEXT,TEXT,TEXT,TEXT,TEXT) TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_verifier_capability_status() TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_nomos_persist_decision(JSONB) TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_submit(JSONB) TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_review_transition(JSONB) TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_claim(JSONB) TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_prepare_claim(JSONB) TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_authorized_read(JSONB) TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_report_outcome(JSONB) TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_owner_unavailability(JSONB) TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_create(JSONB) TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_batch(JSONB) TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_finalize(JSONB) TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_store_insert_lease(JSONB) TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_materialize_expiry(TEXT) TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_startup_login_binding() TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_claim_owner_unavailability(TEXT) TO okg_external_action_api;
  END IF;
  IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname='okg_external_action_function_owner') THEN
    GRANT SELECT ON okg.nomos_identity_providers, okg.nomos_subject_bindings TO okg_external_action_function_owner;
    GRANT SELECT, INSERT ON okg.nomos_decisions TO okg_external_action_function_owner;
    GRANT USAGE, SELECT ON okg.nomos_decisions_decision_row_id_seq TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_session_deployment() TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_assert_deployment(TEXT) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_require_command(JSONB,TEXT[],TEXT[]) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_json_bytes(JSONB) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_logical_json_record(JSONB) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_hash(TEXT,JSONB) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_effect_key(JSONB) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_hmac_sha256(BYTEA,BYTEA) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_api_role_binding_guard() TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_verifier_capability_status() TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_verify_verifier_assertion(JSONB,TEXT,JSONB) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_startup_login_binding() TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_claim_owner_unavailability(TEXT) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_nomos_authority_snapshot(TEXT,TEXT,TEXT,TEXT,TEXT) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_nomos_policy_bundle(TEXT,TEXT,TEXT) TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_require_command(JSONB,TEXT[],TEXT[]) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_json_bytes(JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_logical_json_record(JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_hash(TEXT,JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_effect_key(JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_startup_login_binding() OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_claim_owner_unavailability(TEXT) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_prepare_claim(JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_authorized_read(JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_record_auth_audit(JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_hmac_sha256(BYTEA,BYTEA) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_api_role_binding_guard() OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_verifier_capability_status() OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_verify_verifier_assertion(JSONB,TEXT,JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_nomos_authority_snapshot(TEXT,TEXT,TEXT,TEXT,TEXT) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_nomos_persist_decision(JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_submit(JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_review_transition(JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_claim(JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_report_outcome(JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_owner_unavailability(JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_restore_create(JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_restore_batch(JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_restore_finalize(JSONB) OWNER TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_store_insert_lease(JSONB) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_materialize_expiry(TEXT) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_hash(TEXT,JSONB) TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_hash(TEXT,JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_store_insert_lease(JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_materialize_expiry(TEXT) OWNER TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_nomos_policy_bundle(TEXT,TEXT,TEXT) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_store_insert_lease(JSONB) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_materialize_expiry(TEXT) TO okg_external_action_function_owner;
  END IF;
END
$external_action_semantic_function_grants$;

-- Final restore command implementations are defined after all historical
-- definitions so an upgrade cannot leave the permissive legacy body active.
CREATE OR REPLACE FUNCTION okg.external_action_restore_batch(p_command JSONB)
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,okg
AS $external_action_restore_batch_hardened$
DECLARE bound_deployment TEXT:=okg.external_action_session_deployment();
  session_row okg.external_action_restore_sessions%ROWTYPE;
  header_row okg.external_action_restore_checkpoint_staging%ROWTYPE;
  existing_header okg.external_action_restore_checkpoint_staging%ROWTYPE;
  entry JSONB; entries JSONB; key_value TEXT; previous_key TEXT; ordinal_value BIGINT:=0;
  item_count BIGINT; distinct_count BIGINT; header_checkpoint JSONB; header_entries JSONB;
  request_projection JSONB; expected_request_hash TEXT;
  observed_at TIMESTAMPTZ:=clock_timestamp();
BEGIN
  PERFORM okg.external_action_verifier_capability_status();
  PERFORM okg.external_action_require_command(
    p_command,
    ARRAY['deployment','restore_id','batch_index','request_hash','idempotency_key_hash',
      'operator_separation_subject','restore_attestation_envelope_digest','checkpoints',
      'checkpoint_envelope_digest','proof_digest','checkpoint_payload',
      'authentication_audit_id','nomos_decision_id'],
    ARRAY['deployment','restore_id','batch_index','request_hash','idempotency_key_hash',
      'operator_separation_subject','restore_attestation_envelope_digest','checkpoints',
      'authentication_audit_id','nomos_decision_id']
  );
  IF p_command->>'deployment' IS DISTINCT FROM bound_deployment THEN
    RAISE EXCEPTION 'external action deployment is not bound to session_user' USING ERRCODE='28000';
  END IF;
  IF (p_command->>'batch_index')::bigint<0 OR p_command->>'batch_index' !~ '^(0|[1-9][0-9]*)$' THEN
    RAISE EXCEPTION 'restore batch index is not canonical' USING ERRCODE='22023';
  END IF;
  entries:=p_command->'checkpoints';
  IF jsonb_typeof(entries) IS DISTINCT FROM 'array' OR jsonb_array_length(entries)<1
     OR jsonb_array_length(entries)>256 THEN
    RAISE EXCEPTION 'restore checkpoint batch must contain 1..256 entries' USING ERRCODE='22023';
  END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended('okg.external_action.deployment:'||bound_deployment,0));
  PERFORM okg.external_action_nomos_authority_snapshot(bound_deployment,p_command->>'authentication_audit_id',p_command->>'nomos_decision_id','external_action.restore_reconcile',p_command->>'request_hash');
  SELECT * INTO session_row FROM okg.external_action_restore_sessions
   WHERE deployment_name=bound_deployment AND restore_id=p_command->>'restore_id' FOR UPDATE;
  IF session_row.restore_session_row_id IS NULL OR session_row.status NOT IN ('staging','quarantined') THEN
    RAISE EXCEPTION 'restore session is not accepting checkpoint batches' USING ERRCODE='40001';
  END IF;
  IF session_row.operator_separation_subject IS DISTINCT FROM p_command->>'operator_separation_subject'
     OR session_row.restore_attestation_envelope_digest IS DISTINCT FROM p_command->>'restore_attestation_envelope_digest' THEN
    RAISE EXCEPTION 'restore batch operator or attestation is not bound to the session' USING ERRCODE='42501';
  END IF;
  IF EXISTS (SELECT 1 FROM okg.external_action_restore_completion_receipts
    WHERE deployment_name=bound_deployment AND restore_id=session_row.restore_id) THEN
    RAISE EXCEPTION 'restore session is already complete'
      USING ERRCODE='40001',
        DETAIL=jsonb_build_object(
          'classification','database_restore_terminal_conflict',
          'quarantine_state','clear'
        )::text;
  END IF;
  -- An exact retry is resolved from its immutable header before any effect
  -- key duplicate guard.  The retained request hash is the cryptographic
  -- identity of the already verified batch.
  SELECT * INTO existing_header FROM okg.external_action_restore_checkpoint_staging
   WHERE deployment_name=bound_deployment AND restore_id=session_row.restore_id
     AND batch_index=(p_command->>'batch_index')::bigint FOR UPDATE;
  IF existing_header.checkpoint_staging_row_id IS NOT NULL THEN
    -- Resolve both exact replay and a key/request collision before looking at
    -- effect keys in the new body.  Otherwise a retry with a changed key can
    -- leak the duplicate-effect diagnostic instead of the batch conflict.
    IF existing_header.operator_separation_subject IS DISTINCT FROM p_command->>'operator_separation_subject'
       OR existing_header.idempotency_key_hash IS DISTINCT FROM p_command->>'idempotency_key_hash'
       OR existing_header.request_hash IS DISTINCT FROM p_command->>'request_hash' THEN
      RAISE EXCEPTION 'restore batch replay conflicts with the first operator, key, or request' USING ERRCODE='40001';
    END IF;
    RETURN jsonb_build_object('replay',TRUE,'deployment',bound_deployment,'restore_id',session_row.restore_id,
      'batch_index',existing_header.batch_index,'accepted_checkpoint_count',
      (SELECT count(*) FROM okg.external_action_restore_checkpoint_entries e WHERE e.deployment_name=bound_deployment AND e.restore_id=session_row.restore_id AND e.batch_index=existing_header.batch_index),
      'quarantine_state','blocked');
  END IF;
  FOR entry IN SELECT value FROM jsonb_array_elements(entries) LOOP
    IF jsonb_typeof(entry) IS DISTINCT FROM 'object' THEN
      RAISE EXCEPTION 'restore checkpoint entry must be an object' USING ERRCODE='22023';
    END IF;
    IF EXISTS (SELECT 1 FROM jsonb_object_keys(entry) k
      WHERE k NOT IN ('effect_instance_key','checkpoint_envelope_digest','proof_digest','checkpoint_payload','checkpoint_envelope','proof'))
       OR NOT (entry ? 'effect_instance_key') OR NOT (entry ? 'checkpoint_envelope_digest')
       OR NOT (entry ? 'proof_digest') OR NOT (entry ? 'checkpoint_payload')
       OR NOT (entry ? 'checkpoint_envelope') OR NOT (entry ? 'proof') THEN
      RAISE EXCEPTION 'restore checkpoint entry has missing or extra fields' USING ERRCODE='22023';
    END IF;
    IF jsonb_typeof(entry->'checkpoint_payload') IS DISTINCT FROM 'object'
       OR jsonb_typeof(entry->'checkpoint_envelope') IS DISTINCT FROM 'object'
       OR jsonb_typeof(entry->'proof') IS DISTINCT FROM 'object' THEN
      RAISE EXCEPTION 'restore checkpoint evidence must be objects' USING ERRCODE='22023';
    END IF;
    IF EXISTS (
      SELECT 1 FROM jsonb_object_keys(entry->'checkpoint_payload') k
       WHERE k NOT IN (
         'deployment','effect_instance_key','execution_epoch',
         'ledger_fence_envelope_digest','fenced_ledger_root_digest',
         'fenced_ledger_entry_count','proof_kind','positive_proof_digest',
         'nonmembership_proof_digest','ledger_entry_digest',
         'submission_content_hash','proposal_id','proposal_request_hash',
         'original_payload_hash','approved_payload_hash','action_contract_digest',
         'decision_id','decision_binding_hash','execution_id',
         'execution_binding_hash','okg_claim_handoff_receipt_digest',
         'provider_idempotency_key_digest','journal_phase','effect_started_at',
         'provider_receipt_digest','outcome','outcome_reason_code',
         'outcome_recorded_at','journal_digest','pre_restore_effect_event_head_hash',
         'checkpoint_issued_at'))
      OR EXISTS (
        SELECT 1 FROM unnest(ARRAY[
          'deployment','effect_instance_key','execution_epoch',
          'ledger_fence_envelope_digest','fenced_ledger_root_digest',
          'fenced_ledger_entry_count','proof_kind','positive_proof_digest',
          'nonmembership_proof_digest','ledger_entry_digest',
          'submission_content_hash','proposal_id','proposal_request_hash',
          'original_payload_hash','approved_payload_hash','action_contract_digest',
          'decision_id','decision_binding_hash','execution_id',
          'execution_binding_hash','okg_claim_handoff_receipt_digest',
          'provider_idempotency_key_digest','journal_phase','effect_started_at',
          'provider_receipt_digest','outcome','outcome_reason_code',
          'outcome_recorded_at','journal_digest','pre_restore_effect_event_head_hash',
          'checkpoint_issued_at']::text[]) field
         WHERE NOT (entry->'checkpoint_payload' ? field)) THEN
      RAISE EXCEPTION 'restore checkpoint payload is not the closed signed projection' USING ERRCODE='22023';
    END IF;
    key_value:=entry->>'effect_instance_key';
    IF key_value IS NULL OR btrim(key_value)='' OR (entry->>'checkpoint_envelope_digest') !~ '^sha256:[0-9a-f]{64}$'
       OR (entry->'checkpoint_envelope'->>'envelope_digest') IS DISTINCT FROM entry->>'checkpoint_envelope_digest'
       OR (entry->>'proof_digest') !~ '^sha256:[0-9a-f]{64}$' THEN
      RAISE EXCEPTION 'restore checkpoint digest or effect key is invalid' USING ERRCODE='22023';
    END IF;
    IF previous_key IS NOT NULL AND convert_to(key_value,'UTF8')<=convert_to(previous_key,'UTF8') THEN
      RAISE EXCEPTION 'restore checkpoint effect keys must be sorted and unique' USING ERRCODE='22023';
    END IF;
    IF entry->'checkpoint_payload'->>'proof_kind'='positive'
       AND (entry->'proof'->>'leaf_index' IS NULL
            OR entry->'proof'->>'leaf_index' !~ '^(0|[1-9][0-9]*)$') THEN
      RAISE EXCEPTION 'positive restore proof leaf index is not canonical' USING ERRCODE='22023';
    END IF;
    previous_key:=key_value; ordinal_value:=ordinal_value+1;
  END LOOP;
  SELECT count(*),count(DISTINCT e->>'effect_instance_key') INTO item_count,distinct_count FROM jsonb_array_elements(entries) e;
  IF item_count<>distinct_count OR EXISTS (SELECT 1 FROM jsonb_array_elements(entries) e
    JOIN okg.external_action_restore_checkpoint_entries old ON old.deployment_name=bound_deployment AND old.restore_id=session_row.restore_id AND old.effect_instance_key=e->>'effect_instance_key') THEN
    RAISE EXCEPTION 'restore checkpoint effect key is duplicated' USING ERRCODE='40001';
  END IF;
  SELECT COALESCE(jsonb_agg(jsonb_build_object(
      'effect_instance_key',e->>'effect_instance_key',
      'checkpoint_envelope_digest',e->>'checkpoint_envelope_digest',
      'proof_digest',e->>'proof_digest','checkpoint_payload',e->'checkpoint_payload'-'classification')
      ORDER BY x.ordinality),'[]'::jsonb),
         jsonb_build_object(
           'restore_id',session_row.restore_id,
           'batch_index',(p_command->>'batch_index')::bigint,
           'checkpoints',COALESCE(jsonb_agg(jsonb_build_object(
             'effect_instance_key',e->>'effect_instance_key',
             'checkpoint_envelope_digest',e->>'checkpoint_envelope_digest',
             'proof_digest',e->>'proof_digest') ORDER BY x.ordinality),'[]'::jsonb))
    INTO header_entries,request_projection
    FROM jsonb_array_elements(entries) WITH ORDINALITY AS x(e,ordinality);
  expected_request_hash:=okg.external_action_hash(
    'okg-external-action-restore-batch-request-v1',request_projection);
  IF p_command->>'request_hash' IS DISTINCT FROM expected_request_hash THEN
    RAISE EXCEPTION 'restore batch request hash is not canonical' USING ERRCODE='40001';
  END IF;
  SELECT * INTO existing_header FROM okg.external_action_restore_checkpoint_staging
   WHERE deployment_name=bound_deployment AND restore_id=session_row.restore_id
     AND batch_index=(p_command->>'batch_index')::bigint FOR UPDATE;
  IF existing_header.checkpoint_staging_row_id IS NOT NULL THEN
    IF existing_header.operator_separation_subject IS DISTINCT FROM p_command->>'operator_separation_subject'
       OR existing_header.idempotency_key_hash IS DISTINCT FROM p_command->>'idempotency_key_hash'
       OR existing_header.request_hash IS DISTINCT FROM p_command->>'request_hash'
       OR existing_header.checkpoint_payload IS DISTINCT FROM header_entries THEN
      RAISE EXCEPTION 'restore batch replay conflicts with the first operator, key, or request' USING ERRCODE='40001';
    END IF;
    RETURN jsonb_build_object('replay',TRUE,'deployment',bound_deployment,'restore_id',session_row.restore_id,
      'batch_index',existing_header.batch_index,'accepted_checkpoint_count',
      (SELECT count(*) FROM okg.external_action_restore_checkpoint_entries e WHERE e.deployment_name=bound_deployment AND e.restore_id=session_row.restore_id AND e.batch_index=existing_header.batch_index),
      'quarantine_state','blocked');
  END IF;
  SELECT COALESCE(jsonb_agg(to_jsonb(e->>'checkpoint_envelope_digest') ORDER BY convert_to(e->>'checkpoint_envelope_digest','UTF8')),'[]'::jsonb)
    INTO header_checkpoint FROM jsonb_array_elements(entries) e;
  INSERT INTO okg.external_action_restore_checkpoint_staging
    (deployment_name,restore_id,batch_index,request_hash,checkpoint_envelope_digest,proof_digest,checkpoint_payload,staged_at,store_record,effect_instance_key,operator_separation_subject,idempotency_key_hash)
  VALUES (bound_deployment,session_row.restore_id,(p_command->>'batch_index')::bigint,p_command->>'request_hash',
    okg.external_action_hash('okg-external-action-restore-checkpoint-set-v1',jsonb_build_object('sorted_checkpoint_envelope_digests',header_checkpoint,'count',item_count)),
    okg.external_action_hash('okg-external-action-restore-proof-batch-v1',entries),header_entries,observed_at,p_command,NULL,
    p_command->>'operator_separation_subject',p_command->>'idempotency_key_hash') RETURNING * INTO header_row;
  ordinal_value:=0;
  FOR entry IN SELECT value FROM jsonb_array_elements(entries) LOOP
    ordinal_value:=ordinal_value+1;
    INSERT INTO okg.external_action_restore_checkpoint_entries
      (deployment_name,restore_id,batch_index,ordinal,effect_instance_key,checkpoint_envelope_digest,proof_digest,leaf_index,proof,checkpoint_payload,staged_at)
    VALUES (bound_deployment,session_row.restore_id,header_row.batch_index,ordinal_value-1,entry->>'effect_instance_key',
      entry->>'checkpoint_envelope_digest',entry->>'proof_digest',NULLIF(entry->'proof'->>'leaf_index','')::bigint,entry->'proof',entry->'checkpoint_payload'-'classification',observed_at);
  END LOOP;
  RETURN jsonb_build_object('replay',FALSE,'deployment',bound_deployment,'restore_id',session_row.restore_id,
    'batch_index',header_row.batch_index,'accepted_checkpoint_count',item_count,'quarantine_state','blocked');
END
$external_action_restore_batch_hardened$;

-- Prepare returns the exact immutable inputs which the service must sign on
-- the same connection before calling finalize.  It performs no durable write
-- and therefore incomplete coverage never reserves an idempotency key.
CREATE OR REPLACE FUNCTION okg.external_action_restore_prepare(p_command JSONB)
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,okg
AS $external_action_restore_prepare$
DECLARE checked JSONB; root JSONB; attestation JSONB;
BEGIN
  checked:=okg.external_action_restore_assert_coverage(p_command);
  root:=okg.external_action_restore_event_root(checked->>'deployment');
  RETURN checked || jsonb_build_object(
    'restored_event_root_digest',root->>'digest',
    'restored_event_head_count',(root->>'count')::bigint,
    'completed_at',to_char(clock_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
  );
END
$external_action_restore_prepare$;

CREATE OR REPLACE FUNCTION okg.external_action_restore_finalize(p_command JSONB)
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,okg
AS $external_action_restore_finalize_hardened$
DECLARE bound_deployment TEXT:=okg.external_action_session_deployment();
  session_row okg.external_action_restore_sessions%ROWTYPE;
  attestation_row okg.external_action_restore_attestations%ROWTYPE;
  fence_row okg.external_action_restore_fences%ROWTYPE;
  existing_final okg.external_action_restore_finalize_idempotency%ROWTYPE;
  completion_row okg.external_action_restore_completion_receipts%ROWTYPE;
  checked JSONB; projection JSONB; unsigned JSONB; recovery_projection JSONB;
  recovery_set JSONB; pre_root JSONB; post_root JSONB; entry JSONB; payload JSONB;
  old_sequence BIGINT; previous_hash TEXT; event_hash_value TEXT; recovery_digest_value TEXT;
  effective_value TIMESTAMPTZ; recorded_value TIMESTAMPTZ; completed_value TIMESTAMPTZ;
  classification TEXT; source_proposal TEXT; source_decision TEXT; source_execution TEXT;
  response JSONB; coverage_command JSONB; operator_value TEXT; key_value TEXT; request_value TEXT;
  observed_at TIMESTAMPTZ:=clock_timestamp();
BEGIN
  PERFORM okg.external_action_require_command(
    p_command,
    ARRAY['deployment','restore_id','restore_attestation_envelope_digest','expected_inventory_digest',
      'expected_inventory_count','expected_checkpoint_set_digest','expected_checkpoint_count',
      'operator_separation_subject','idempotency_key_hash','request_hash','completion_receipt_envelope_digest',
      'completion_receipt_protected_b64','completion_receipt_payload_b64','completion_receipt_signature_b64',
      'completion_receipt_projection','completion_receipt_projection_digest','authentication_audit_id',
      'nomos_decision_id','service_mode','quarantine_state'],
    ARRAY['deployment','restore_id','restore_attestation_envelope_digest','expected_inventory_digest',
      'expected_inventory_count','expected_checkpoint_set_digest','expected_checkpoint_count',
      'completion_receipt_envelope_digest','completion_receipt_protected_b64','completion_receipt_payload_b64',
      'completion_receipt_signature_b64','completion_receipt_projection','completion_receipt_projection_digest',
      'authentication_audit_id','nomos_decision_id']
  );
  IF p_command->>'deployment' IS DISTINCT FROM bound_deployment THEN
    RAISE EXCEPTION 'external action deployment is not bound to session_user' USING ERRCODE='28000';
  END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended('okg.external_action.deployment:'||bound_deployment,0));
  SELECT * INTO session_row FROM okg.external_action_restore_sessions
   WHERE deployment_name=bound_deployment AND restore_id=p_command->>'restore_id' FOR UPDATE;
  IF session_row.restore_session_row_id IS NULL THEN
    RAISE EXCEPTION 'restore session is not finalizable' USING ERRCODE='40001';
  END IF;
  operator_value:=COALESCE(p_command->>'operator_separation_subject',session_row.operator_separation_subject);
  key_value:=COALESCE(p_command->>'idempotency_key_hash',session_row.idempotency_key_hash);
  request_value:=COALESCE(p_command->>'request_hash',session_row.request_hash);
  IF operator_value IS DISTINCT FROM session_row.operator_separation_subject THEN
    RAISE EXCEPTION 'restore finalize operator is not the session operator' USING ERRCODE='42501';
  END IF;
  SELECT * INTO existing_final FROM okg.external_action_restore_finalize_idempotency
   WHERE deployment_name=bound_deployment AND restore_id=session_row.restore_id FOR SHARE;
  IF existing_final.finalize_idempotency_row_id IS NOT NULL THEN
    IF existing_final.operator_separation_subject IS DISTINCT FROM operator_value
       OR existing_final.idempotency_key_hash IS DISTINCT FROM key_value
       OR existing_final.request_hash IS DISTINCT FROM request_value THEN
      RAISE EXCEPTION 'restore finalize replay conflicts with the first operator, key, or request' USING ERRCODE='40001';
    END IF;
    RETURN convert_from(existing_final.response_body_jcs,'UTF8')::jsonb;
  END IF;
  SELECT * INTO completion_row FROM okg.external_action_restore_completion_receipts
   WHERE deployment_name=bound_deployment AND restore_id=session_row.restore_id FOR SHARE;
  IF completion_row.completion_receipt_row_id IS NOT NULL THEN
    RAISE EXCEPTION 'restore completion exists without its finalize idempotency record' USING ERRCODE='40001';
  END IF;
  coverage_command:=jsonb_build_object('deployment',bound_deployment,'restore_id',p_command->>'restore_id',
    'restore_attestation_envelope_digest',p_command->>'restore_attestation_envelope_digest',
    'expected_inventory_digest',p_command->>'expected_inventory_digest','expected_inventory_count',p_command->>'expected_inventory_count',
    'expected_checkpoint_set_digest',p_command->>'expected_checkpoint_set_digest','expected_checkpoint_count',p_command->>'expected_checkpoint_count');
  checked:=okg.external_action_restore_assert_coverage(coverage_command);
  SELECT * INTO attestation_row FROM okg.external_action_restore_attestations
   WHERE deployment_name=bound_deployment AND envelope_digest=session_row.restore_attestation_envelope_digest FOR SHARE;
  pre_root:=okg.external_action_restore_event_root(bound_deployment);
  IF pre_root->>'digest' IS DISTINCT FROM attestation_row.pre_restore_event_root_digest
     OR (pre_root->>'count')::bigint IS DISTINCT FROM attestation_row.pre_restore_event_head_count THEN
    RAISE EXCEPTION 'restore event root is stale or does not match the attestation' USING ERRCODE='40001';
  END IF;
  projection:=p_command->'completion_receipt_projection';
  IF jsonb_typeof(projection) IS DISTINCT FROM 'object' OR
     projection->>'deployment' IS DISTINCT FROM bound_deployment OR
     projection->>'restore_id' IS DISTINCT FROM session_row.restore_id OR
     projection->>'restore_attestation_envelope_digest' IS DISTINCT FROM session_row.restore_attestation_envelope_digest OR
     projection->>'pre_restore_event_root_digest' IS DISTINCT FROM attestation_row.pre_restore_event_root_digest OR
     projection->>'pre_restore_event_head_count' IS DISTINCT FROM attestation_row.pre_restore_event_head_count THEN
    RAISE EXCEPTION 'completion receipt projection is not bound to the restore' USING ERRCODE='40001';
  END IF;
  IF okg.external_action_hash('okg-external-action-restore-completion-receipt-v1',projection)
     IS DISTINCT FROM p_command->>'completion_receipt_projection_digest' THEN
    RAISE EXCEPTION 'completion receipt projection digest is invalid' USING ERRCODE='22023';
  END IF;
  unsigned:=jsonb_build_object('protected_b64',p_command->>'completion_receipt_protected_b64',
    'payload_b64',p_command->>'completion_receipt_payload_b64','signature_b64',p_command->>'completion_receipt_signature_b64');
  IF p_command->>'completion_receipt_envelope_digest' IS DISTINCT FROM
     okg.external_action_hash('okg-external-action-restore-completion-receipt-envelope-v1',unsigned) THEN
    RAISE EXCEPTION 'completion receipt envelope digest is invalid' USING ERRCODE='22023';
  END IF;
  IF p_command->>'completion_receipt_protected_b64' !~ '^[A-Za-z0-9_-]+$'
     OR p_command->>'completion_receipt_payload_b64' !~ '^[A-Za-z0-9_-]+$'
     OR p_command->>'completion_receipt_signature_b64' !~ '^[A-Za-z0-9_-]+$' THEN
    RAISE EXCEPTION 'completion receipt envelope encoding is invalid' USING ERRCODE='22023';
  END IF;
  -- Each checkpoint carries the already verified signed worker projection.
  -- SQL validates its immutable relational binding and recovery classification;
  -- detached signature/proof verification remains in the Python authority.
  FOR entry IN SELECT e FROM okg.external_action_restore_checkpoint_entries e
    WHERE e.deployment_name=bound_deployment AND e.restore_id=session_row.restore_id
    ORDER BY convert_to(e.effect_instance_key,'UTF8') LOOP
    payload:=entry.e.checkpoint_payload;
    classification:=payload->>'classification';
    IF classification NOT IN ('database_restore_no_effect_started','database_restore_terminal_review_confirmed','database_restore_effect_uncertain','database_restore_succeeded','database_restore_failed','database_restore_terminal_confirmed','database_restore_terminal_conflict') THEN
      RAISE EXCEPTION 'restore checkpoint classification is invalid' USING ERRCODE='40001';
    END IF;
    effective_value:=COALESCE(NULLIF(payload->>'effective_at','')::timestamptz,observed_at);
    recorded_value:=observed_at;
    source_proposal:=NULLIF(payload->>'source_proposal_id','');
    source_decision:=NULLIF(payload->>'source_decision_id','');
    source_execution:=NULLIF(payload->>'source_execution_id','');
    recovery_projection:=jsonb_build_object('deployment',bound_deployment,'old_epoch',attestation_row.old_epoch,
      'new_epoch',attestation_row.new_epoch,'effect_instance_key',entry.e.effect_instance_key,
      'submission_content_hash',COALESCE(payload->>'submission_content_hash',''),'checkpoint_digest',entry.e.checkpoint_envelope_digest,
      'restore_attestation_digest',attestation_row.envelope_digest,'classification',classification,
      'source_proposal_id',source_proposal,'source_decision_id',source_decision,'source_execution_id',source_execution,
      'recovered_review_state',NULLIF(payload->>'recovered_review_state',''),'recovered_execution_state',NULLIF(payload->>'recovered_execution_state',''),
      'recovered_outcome',NULLIF(payload->>'recovered_outcome',''),'effective_at',to_char(effective_value AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
      'recorded_at',to_char(recorded_value AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),'pre_restore_effect_event_head_hash',NULLIF(payload->>'pre_restore_effect_event_head_hash',''));
    recovery_digest_value:=okg.external_action_hash('okg-external-action-restore-recovery-v1',recovery_projection);
    INSERT INTO okg.external_action_restore_recoveries
      (deployment_name,old_epoch,new_epoch,effect_instance_key,submission_content_hash,checkpoint_digest,restore_attestation_digest,classification,source_proposal_id,source_decision_id,source_execution_id,recovered_review_state,recovered_execution_state,recovered_outcome,effective_at,recorded_at,pre_restore_effect_event_head_hash,signed_worker_checkpoint_digest,recovery_digest,store_record)
    VALUES (bound_deployment,attestation_row.old_epoch,attestation_row.new_epoch,entry.e.effect_instance_key,
      COALESCE(payload->>'submission_content_hash',''),entry.e.checkpoint_envelope_digest,attestation_row.envelope_digest,classification,
      source_proposal,source_decision,source_execution,NULLIF(payload->>'recovered_review_state',''),NULLIF(payload->>'recovered_execution_state',''),NULLIF(payload->>'recovered_outcome',''),effective_value,recorded_value,NULLIF(payload->>'pre_restore_effect_event_head_hash',''),entry.e.checkpoint_envelope_digest,recovery_digest_value,payload);
    PERFORM pg_advisory_xact_lock(hashtextextended('okg.external_action.effect:'||entry.e.effect_instance_key,0));
    SELECT COALESCE(max(sequence),0), (array_agg(event_hash ORDER BY sequence))[array_length(array_agg(event_hash ORDER BY sequence),1)]
      INTO old_sequence,previous_hash FROM okg.external_action_events
     WHERE deployment_name=bound_deployment AND effect_instance_key=entry.e.effect_instance_key;
    event_hash_value:=okg.external_action_hash('okg-external-action-event-v1',jsonb_build_object(
      'deployment',bound_deployment,'proposal_id',source_proposal,'execution_epoch',attestation_row.new_epoch,
      'effect_instance_key',entry.e.effect_instance_key,'sequence',old_sequence+1,'lifecycle_version',1,
      'event_type','restore_recovery','actor_kind','restore_operator','actor_principal',operator_value,
      'actor_separation_subject',operator_value,'authentication_audit_ref',p_command->>'authentication_audit_id',
      'nomos_decision_ref',p_command->>'nomos_decision_id','restore_recovery_digest',recovery_digest_value,
      'effective_at',to_char(effective_value AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
      'recorded_at',to_char(recorded_value AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),'previous_event_hash',previous_hash));
    INSERT INTO okg.external_action_events
      (deployment_name,proposal_id,execution_epoch,effect_instance_key,sequence,lifecycle_version,event_type,actor_kind,actor_principal,actor_separation_subject,authentication_audit_ref,nomos_decision_ref,restore_recovery_digest,effective_at,recorded_at,previous_event_hash,event_hash,store_record)
    VALUES (bound_deployment,source_proposal,attestation_row.new_epoch,entry.e.effect_instance_key,old_sequence+1,1,'restore_recovered','restore_operator',operator_value,operator_value,p_command->>'authentication_audit_id',p_command->>'nomos_decision_id',recovery_digest_value,effective_value,recorded_value,previous_hash,event_hash_value,jsonb_build_object('checkpoint_digest',entry.e.checkpoint_envelope_digest));
  END LOOP;
  post_root:=okg.external_action_restore_event_root(bound_deployment);
  SELECT jsonb_build_object('sorted_restore_recovery_digests',COALESCE(jsonb_agg(to_jsonb(recovery_digest) ORDER BY convert_to(recovery_digest,'UTF8')),'[]'::jsonb),'count',count(*)) INTO recovery_set
    FROM okg.external_action_restore_recoveries WHERE deployment_name=bound_deployment AND restore_attestation_digest=attestation_row.envelope_digest;
  recovery_set:=recovery_set||jsonb_build_object('digest',okg.external_action_hash('okg-external-action-restore-recovery-set-v1',recovery_set));
  IF projection->>'restored_event_root_digest' IS DISTINCT FROM post_root->>'digest'
     OR (projection->>'restored_event_head_count')::bigint IS DISTINCT FROM (post_root->>'count')::bigint
     OR projection->>'recovery_set_digest' IS DISTINCT FROM recovery_set->>'digest'
     OR (projection->>'recovery_count')::bigint IS DISTINCT FROM (recovery_set->>'count')::bigint THEN
    RAISE EXCEPTION 'completion receipt projection does not match durable recovery result' USING ERRCODE='40001';
  END IF;
  completed_value:=(projection->>'completed_at')::timestamptz;
  IF completed_value IS NULL THEN RAISE EXCEPTION 'completion receipt has no completed_at' USING ERRCODE='22023'; END IF;
  response:=jsonb_build_object('replay',FALSE,'deployment',bound_deployment,'restore_id',session_row.restore_id,'state','complete','quarantine_state','cleared','service_mode','normal','recovery_count',(recovery_set->>'count')::bigint,'recovery_set_digest',recovery_set->>'digest','restore_completion_receipt',jsonb_build_object('envelope_digest',p_command->>'completion_receipt_envelope_digest'));
  INSERT INTO okg.external_action_restore_completion_receipts
    (deployment_name,restore_id,restore_attestation_envelope_digest,completion_receipt_envelope_digest,completed_at,authentication_audit_id,nomos_decision_id,store_record,protected_b64,payload_b64,signature_b64,completion_receipt_projection)
  VALUES (bound_deployment,session_row.restore_id,attestation_row.envelope_digest,p_command->>'completion_receipt_envelope_digest',completed_value,p_command->>'authentication_audit_id',p_command->>'nomos_decision_id',response,p_command->>'completion_receipt_protected_b64',p_command->>'completion_receipt_payload_b64',p_command->>'completion_receipt_signature_b64',projection);
  INSERT INTO okg.external_action_restore_finalize_idempotency
    (deployment_name,restore_id,operator_separation_subject,idempotency_key_hash,request_hash,response_body_jcs,completion_receipt_envelope_digest)
  VALUES (bound_deployment,session_row.restore_id,operator_value,key_value,request_value,okg.external_action_json_bytes(response),p_command->>'completion_receipt_envelope_digest');
  SELECT * INTO fence_row FROM okg.external_action_restore_fences WHERE deployment_name=bound_deployment FOR UPDATE;
  IF fence_row.deployment_name IS NULL OR fence_row.highest_accepted_epoch>attestation_row.new_epoch THEN
    RAISE EXCEPTION 'restore fence changed while finalizing' USING ERRCODE='40001';
  END IF;
  UPDATE okg.external_action_restore_fences
     SET highest_accepted_epoch=attestation_row.new_epoch,lease_digest=attestation_row.new_epoch_lease_digest,
         quarantine_state='clear',service_mode='normal',restore_attestation_digest=attestation_row.envelope_digest,
         accepted_at=completed_value,mode_changed_at=completed_value
   WHERE deployment_name=bound_deployment;
  RETURN response;
END
$external_action_restore_finalize_hardened$;

CREATE OR REPLACE FUNCTION okg.external_action_restore_lifecycle_gate()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,okg
AS $external_action_restore_lifecycle_gate$
DECLARE deployment_value TEXT; epoch_value BIGINT; fence_row okg.external_action_restore_fences%ROWTYPE;
BEGIN
  deployment_value:=NEW.deployment_name;
  epoch_value:=COALESCE(NEW.execution_epoch,0);
  -- Serialize every lifecycle insert with startup rotation and restore
  -- clearance.  A shared fence read alone permits an insert to slip between
  -- the read and a concurrent mode switch.
  PERFORM pg_advisory_xact_lock(
    hashtextextended('okg.external_action.deployment:'||deployment_value,0)
  );
  SELECT * INTO fence_row FROM okg.external_action_restore_fences
   WHERE deployment_name=deployment_value FOR SHARE;
  IF fence_row.deployment_name IS NOT NULL AND (fence_row.service_mode<>'normal' OR fence_row.quarantine_state<>'clear') THEN
    RAISE EXCEPTION 'external action lifecycle is quarantined for restore' USING ERRCODE='40001';
  END IF;
  IF fence_row.deployment_name IS NOT NULL AND epoch_value<fence_row.highest_accepted_epoch THEN
    RAISE EXCEPTION 'external action lifecycle epoch is stale' USING ERRCODE='40001';
  END IF;
  RETURN NEW;
END
$external_action_restore_lifecycle_gate$;
DROP TRIGGER IF EXISTS external_action_proposal_restore_gate ON okg.external_action_proposals;
CREATE TRIGGER external_action_proposal_restore_gate BEFORE INSERT ON okg.external_action_proposals
FOR EACH ROW EXECUTE FUNCTION okg.external_action_restore_lifecycle_gate();
DROP TRIGGER IF EXISTS external_action_decision_restore_gate ON okg.external_action_decisions;
CREATE TRIGGER external_action_decision_restore_gate BEFORE INSERT ON okg.external_action_decisions
FOR EACH ROW EXECUTE FUNCTION okg.external_action_restore_lifecycle_gate();
DROP TRIGGER IF EXISTS external_action_execution_restore_gate ON okg.external_action_executions;
CREATE TRIGGER external_action_execution_restore_gate BEFORE INSERT ON okg.external_action_executions
FOR EACH ROW EXECUTE FUNCTION okg.external_action_restore_lifecycle_gate();

-- Restore hardening.  The public API verifies the six signed envelopes and
-- passes their immutable projections to this database boundary.  SQL does
-- not pretend to verify detached signatures or Merkle proofs; it binds the
-- verified digests, inventory, and checkpoint projections transactionally and
-- refuses to clear quarantine unless all relational coverage checks pass.
CREATE OR REPLACE FUNCTION okg.external_action_restore_inventory_digest(p_inventory JSONB)
RETURNS TEXT
LANGUAGE plpgsql IMMUTABLE SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $external_action_restore_inventory_digest$
DECLARE sorted_inventory JSONB; item TEXT; previous_item TEXT;
BEGIN
  IF jsonb_typeof(p_inventory) IS DISTINCT FROM 'array' THEN
    RAISE EXCEPTION 'restore inventory must be an array' USING ERRCODE='22023';
  END IF;
  FOR item IN SELECT value FROM jsonb_array_elements_text(p_inventory) LOOP
    IF item IS NULL OR btrim(item)='' THEN
      RAISE EXCEPTION 'restore inventory contains an empty effect key' USING ERRCODE='22023';
    END IF;
    IF previous_item IS NOT NULL AND convert_to(item,'UTF8') <= convert_to(previous_item,'UTF8') THEN
      RAISE EXCEPTION 'restore inventory must be sorted and unique' USING ERRCODE='22023';
    END IF;
    previous_item := item;
  END LOOP;
  SELECT COALESCE(jsonb_agg(to_jsonb(value) ORDER BY convert_to(value #>> '{}','UTF8')),'[]'::jsonb)
    INTO sorted_inventory FROM jsonb_array_elements(p_inventory);
  IF sorted_inventory IS DISTINCT FROM p_inventory THEN
    RAISE EXCEPTION 'restore inventory must be canonically sorted' USING ERRCODE='22023';
  END IF;
  RETURN okg.external_action_hash(
    'okg-external-action-restore-inventory-v1',
    jsonb_build_object('sorted_effect_instance_keys',p_inventory,'count',jsonb_array_length(p_inventory))
  );
END
$external_action_restore_inventory_digest$;

-- Return the deployment-owned effect inventory used to bind a restore
-- attestation.  The restore id is an audit namespace only: the source of
-- truth is every persisted effect key in this deployment, including keys
-- whose current projection is terminal or partially recovered.  The caller
-- cannot provide or narrow this set.
CREATE OR REPLACE FUNCTION okg.external_action_restore_inventory(p_restore_id TEXT)
RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $external_action_restore_inventory$
DECLARE bound_deployment TEXT:=okg.external_action_session_deployment();
  inventory JSONB; inventory_count BIGINT; inventory_digest TEXT; old_epoch BIGINT;
BEGIN
  PERFORM okg.external_action_verifier_capability_status();
  IF p_restore_id IS NULL OR btrim(p_restore_id)='' THEN
    RAISE EXCEPTION 'restore inventory requires a restore id' USING ERRCODE='22023';
  END IF;
  SELECT highest_accepted_epoch INTO old_epoch
    FROM okg.external_action_restore_fences
   WHERE deployment_name=bound_deployment;
  IF old_epoch IS NULL THEN
    RAISE EXCEPTION 'restore inventory requires a persisted startup fence'
      USING ERRCODE='40001';
  END IF;
  -- After a rotation the fence points at the new epoch while the restore
  -- inventory belongs to the greatest persisted pre-rotation epoch.  On a
  -- renewal (or a first-use fence) that is the fence epoch itself.
  SELECT COALESCE(max(execution_epoch), old_epoch) INTO old_epoch
    FROM (
      SELECT execution_epoch FROM okg.external_action_proposals
       WHERE deployment_name=bound_deployment AND execution_epoch < old_epoch
      UNION ALL
      SELECT execution_epoch FROM okg.external_action_executions
       WHERE deployment_name=bound_deployment AND execution_epoch < old_epoch
      UNION ALL
      SELECT execution_epoch FROM okg.external_action_events
       WHERE deployment_name=bound_deployment AND execution_epoch < old_epoch
    ) epochs;
  SELECT COALESCE(jsonb_agg(to_jsonb(effect_instance_key)
      ORDER BY convert_to(effect_instance_key,'UTF8')),'[]'::jsonb)
    INTO inventory
    FROM (
      SELECT effect_instance_key FROM okg.external_action_proposals
       WHERE deployment_name=bound_deployment AND execution_epoch=old_epoch
      UNION
      SELECT d.effect_instance_key FROM okg.external_action_decisions d
       JOIN okg.external_action_proposals p ON p.deployment_name=d.deployment_name AND p.proposal_id=d.proposal_id
       WHERE d.deployment_name=bound_deployment AND p.execution_epoch=old_epoch
      UNION
      SELECT effect_instance_key FROM okg.external_action_executions
       WHERE deployment_name=bound_deployment AND execution_epoch=old_epoch
      UNION
      SELECT o.effect_instance_key FROM okg.external_action_outcomes o
       JOIN okg.external_action_executions e ON e.deployment_name=o.deployment_name AND e.execution_id=o.execution_id
       WHERE o.deployment_name=bound_deployment AND e.execution_epoch=old_epoch
      UNION
      SELECT effect_instance_key FROM okg.external_action_events
       WHERE deployment_name=bound_deployment AND execution_epoch=old_epoch
    ) owned_keys;
  inventory_count:=jsonb_array_length(inventory);
  inventory_digest:=okg.external_action_restore_inventory_digest(inventory);
  RETURN jsonb_build_object(
    'deployment',bound_deployment,'restore_id',p_restore_id,
    'sorted_effect_instance_keys',inventory,'count',inventory_count,
    'digest',inventory_digest);
END
$external_action_restore_inventory$;

CREATE OR REPLACE FUNCTION okg.external_action_restore_checkpoint_set_digest(p_restore_id TEXT)
RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $external_action_restore_checkpoint_set_digest$
DECLARE digests JSONB; count_value BIGINT;
BEGIN
  SELECT count(*)::bigint,
         COALESCE(jsonb_agg(to_jsonb(checkpoint_envelope_digest)
           ORDER BY convert_to(checkpoint_envelope_digest,'UTF8')),'[]'::jsonb)
    INTO count_value, digests
    FROM okg.external_action_restore_checkpoint_entries
   WHERE deployment_name=okg.external_action_session_deployment() AND restore_id=p_restore_id;
  RETURN jsonb_build_object(
    'sorted_checkpoint_envelope_digests',digests,
    'count',count_value,
    'digest',okg.external_action_hash(
      'okg-external-action-restore-checkpoint-set-v1',
      jsonb_build_object('sorted_checkpoint_envelope_digests',digests,'count',count_value)
    )
  );
END
$external_action_restore_checkpoint_set_digest$;

CREATE OR REPLACE FUNCTION okg.external_action_restore_event_root(p_deployment TEXT)
RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $external_action_restore_event_root$
DECLARE heads JSONB; count_value BIGINT;
BEGIN
  SELECT count(*)::bigint,
         COALESCE(jsonb_agg(jsonb_build_object(
           'effect_instance_key',effect_instance_key,'sequence',sequence,'event_hash',event_hash
         ) ORDER BY convert_to(effect_instance_key,'UTF8')),'[]'::jsonb)
    INTO count_value,heads
    FROM (
      SELECT DISTINCT ON (effect_instance_key) effect_instance_key,sequence,event_hash
        FROM okg.external_action_events
       WHERE deployment_name=p_deployment
       ORDER BY effect_instance_key,sequence DESC
    ) latest;
  RETURN jsonb_build_object(
    'sorted_event_head_entries',heads,'count',count_value,
    'digest',okg.external_action_hash(
      'okg-external-action-restore-event-root-v1',
      jsonb_build_object('sorted_event_head_entries',heads,'count',count_value)
    )
  );
END
$external_action_restore_event_root$;

CREATE OR REPLACE FUNCTION okg.external_action_restore_assert_coverage(p_command JSONB)
RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $external_action_restore_assert_coverage$
DECLARE bound_deployment TEXT:=okg.external_action_session_deployment();
  session_row okg.external_action_restore_sessions%ROWTYPE;
  attestation_row okg.external_action_restore_attestations%ROWTYPE;
  fence_row okg.external_action_restore_fences%ROWTYPE;
  keys JSONB; inventory JSONB; db_inventory JSONB; derived_inventory JSONB; checkpoint_set JSONB; count_value BIGINT;
  inventory_count BIGINT; derived_inventory_count BIGINT; derived_inventory_digest TEXT;
  expected_inventory_digest TEXT; expected_checkpoint_digest TEXT; pre_root JSONB;
  positive_count BIGINT; positive_distinct_count BIGINT; positive_min BIGINT; positive_max BIGINT; positive_previous TEXT; last_positive_key TEXT;
BEGIN
  PERFORM okg.external_action_require_command(
    p_command,
    ARRAY['deployment','restore_id','restore_attestation_envelope_digest',
      'expected_inventory_digest','expected_inventory_count',
      'expected_checkpoint_set_digest','expected_checkpoint_count'],
    ARRAY['deployment','restore_id','restore_attestation_envelope_digest',
      'expected_inventory_digest','expected_inventory_count',
      'expected_checkpoint_set_digest','expected_checkpoint_count']
  );
  IF p_command->>'deployment' IS DISTINCT FROM bound_deployment THEN
    RAISE EXCEPTION 'external action deployment is not bound to session_user' USING ERRCODE='28000';
  END IF;
  SELECT * INTO session_row FROM okg.external_action_restore_sessions
   WHERE deployment_name=bound_deployment AND restore_id=p_command->>'restore_id' FOR UPDATE;
  IF session_row.restore_session_row_id IS NULL THEN
    RAISE EXCEPTION 'restore session is not finalizable' USING ERRCODE='40001';
  END IF;
  SELECT * INTO fence_row FROM okg.external_action_restore_fences
   WHERE deployment_name=bound_deployment FOR UPDATE;
  IF fence_row.deployment_name IS NULL OR fence_row.service_mode<>'restore_only'
     OR fence_row.quarantine_state<>'blocked' THEN
    RAISE EXCEPTION 'restore fence is not blocked in restore_only mode' USING ERRCODE='40001';
  END IF;
  IF session_row.restore_attestation_envelope_digest IS DISTINCT FROM
     p_command->>'restore_attestation_envelope_digest' THEN
    RAISE EXCEPTION 'restore attestation does not match session' USING ERRCODE='40001';
  END IF;
  SELECT * INTO attestation_row FROM okg.external_action_restore_attestations
   WHERE deployment_name=bound_deployment AND envelope_digest=session_row.restore_attestation_envelope_digest
   FOR SHARE;
  IF attestation_row.attestation_row_id IS NULL THEN
    RAISE EXCEPTION 'restore attestation projection is unavailable' USING ERRCODE='40001';
  END IF;
  pre_root:=okg.external_action_restore_event_root(bound_deployment);
  IF pre_root->>'digest' IS DISTINCT FROM attestation_row.pre_restore_event_root_digest
     OR (pre_root->>'count')::bigint IS DISTINCT FROM attestation_row.pre_restore_event_head_count THEN
    RAISE EXCEPTION 'restore pre-event root or count is stale' USING ERRCODE='40001';
  END IF;
  IF (p_command->>'expected_inventory_count')::bigint<>attestation_row.effect_inventory_count
     OR p_command->>'expected_inventory_digest'<>attestation_row.effect_inventory_digest
     OR (p_command->>'expected_checkpoint_count')::bigint<>attestation_row.worker_checkpoint_count
     OR p_command->>'expected_checkpoint_set_digest'<>attestation_row.worker_checkpoint_set_digest THEN
    RAISE EXCEPTION 'restore attestation root or count disagrees with finalize request' USING ERRCODE='40001';
  END IF;
  inventory:=COALESCE(session_row.effect_inventory,'[]'::jsonb);
  inventory_count:=jsonb_array_length(inventory);
  expected_inventory_digest:=okg.external_action_restore_inventory_digest(inventory);
  IF inventory_count<>session_row.effect_inventory_count
     OR session_row.effect_inventory_digest IS DISTINCT FROM expected_inventory_digest THEN
    RAISE EXCEPTION 'restore database inventory projection is not canonical' USING ERRCODE='40001';
  END IF;
  SELECT count(*)::bigint,
         COALESCE(jsonb_agg(to_jsonb(effect_instance_key)
           ORDER BY convert_to(effect_instance_key,'UTF8')),'[]'::jsonb)
    INTO count_value,keys
    FROM okg.external_action_restore_checkpoint_entries
   WHERE deployment_name=bound_deployment AND restore_id=session_row.restore_id;
  IF count_value<>attestation_row.worker_checkpoint_count THEN
    RAISE EXCEPTION 'restore checkpoint coverage is incomplete or has an extra effect key'
      USING ERRCODE='40001',
        DETAIL=jsonb_build_object('code','incomplete_restore_coverage',
          'quarantine_state','blocked',
          'expected_checkpoint_count',attestation_row.worker_checkpoint_count,
          'accepted_checkpoint_count',count_value)::text;
  END IF;
  checkpoint_set:=okg.external_action_restore_checkpoint_set_digest(session_row.restore_id);
  expected_checkpoint_digest:=checkpoint_set->>'digest';
  IF checkpoint_set->>'count'<>(p_command->>'expected_checkpoint_count')
     OR expected_checkpoint_digest<>p_command->>'expected_checkpoint_set_digest' THEN
    RAISE EXCEPTION 'restore checkpoint set digest or count is not exact' USING ERRCODE='40001';
  END IF;
  IF EXISTS (
    SELECT 1 FROM okg.external_action_restore_checkpoint_entries e
     WHERE e.deployment_name=bound_deployment AND e.restore_id=session_row.restore_id
       AND (jsonb_typeof(e.checkpoint_payload) IS DISTINCT FROM 'object'
            OR e.checkpoint_payload->>'proof_kind' NOT IN ('positive','nonmembership'))
  ) THEN
    RAISE EXCEPTION 'restore checkpoint lacks a verified recovery projection' USING ERRCODE='40001';
  END IF;
  IF EXISTS (
    SELECT 1 FROM okg.external_action_restore_checkpoint_entries e
     WHERE e.deployment_name=bound_deployment AND e.restore_id=session_row.restore_id
       AND (
         e.checkpoint_payload->>'deployment' IS DISTINCT FROM bound_deployment
         OR e.checkpoint_payload->>'effect_instance_key' IS DISTINCT FROM e.effect_instance_key
         OR e.checkpoint_payload->>'execution_epoch' IS DISTINCT FROM attestation_row.old_epoch::text
         OR e.checkpoint_payload->>'ledger_fence_envelope_digest' IS DISTINCT FROM attestation_row.ledger_fence_envelope_digest
         OR e.checkpoint_payload->>'fenced_ledger_root_digest' IS DISTINCT FROM attestation_row.fenced_ledger_root_digest
         OR e.checkpoint_payload->>'fenced_ledger_entry_count' IS DISTINCT FROM attestation_row.fenced_ledger_entry_count::text
         OR e.checkpoint_payload->>'checkpoint_issued_at' IS NULL
         OR e.checkpoint_payload->>'submission_content_hash' IS NULL
         OR (e.checkpoint_payload->>'proof_kind'='positive'
             AND e.checkpoint_payload->>'positive_proof_digest' IS DISTINCT FROM e.proof_digest)
         OR (e.checkpoint_payload->>'proof_kind'='nonmembership'
             AND e.checkpoint_payload->>'nonmembership_proof_digest' IS DISTINCT FROM e.proof_digest)
       )
  ) THEN
    RAISE EXCEPTION 'restore checkpoint is not bound to the attested fence projection' USING ERRCODE='40001';
  END IF;
  IF EXISTS (
    SELECT 1 FROM okg.external_action_restore_checkpoint_entries e
     WHERE e.deployment_name=bound_deployment AND e.restore_id=session_row.restore_id
       AND e.checkpoint_payload->>'proof_kind'='positive'
       AND (e.leaf_index IS NULL OR e.leaf_index < 0)
  ) THEN
    RAISE EXCEPTION 'positive checkpoint index is not canonical decimal' USING ERRCODE='40001';
  END IF;
  SELECT count(*) FILTER (WHERE e.checkpoint_payload->>'proof_kind'='positive'),
         count(DISTINCT e.leaf_index) FILTER (WHERE e.checkpoint_payload->>'proof_kind'='positive'),
         min(e.leaf_index) FILTER (WHERE e.checkpoint_payload->>'proof_kind'='positive'),
         max(e.leaf_index) FILTER (WHERE e.checkpoint_payload->>'proof_kind'='positive')
    INTO positive_count,positive_distinct_count,positive_min,positive_max
    FROM okg.external_action_restore_checkpoint_entries e
   WHERE e.deployment_name=bound_deployment AND e.restore_id=session_row.restore_id;
  IF positive_count<>attestation_row.fenced_ledger_entry_count
     OR positive_distinct_count<>positive_count
     OR (positive_count>0 AND (positive_min<>0 OR positive_max<>positive_count-1)) THEN
    RAISE EXCEPTION 'positive checkpoint indexes do not cover the fenced ledger exactly' USING ERRCODE='40001';
  END IF;
  last_positive_key:=NULL;
  FOR positive_previous IN SELECT e.effect_instance_key FROM okg.external_action_restore_checkpoint_entries e
    WHERE e.deployment_name=bound_deployment AND e.restore_id=session_row.restore_id
      AND e.checkpoint_payload->>'proof_kind'='positive'
    ORDER BY e.leaf_index LOOP
    IF positive_previous IS NULL OR btrim(positive_previous)='' THEN
      RAISE EXCEPTION 'positive checkpoint has no effect key' USING ERRCODE='40001';
    END IF;
    IF last_positive_key IS NOT NULL AND convert_to(positive_previous,'UTF8')<=convert_to(last_positive_key,'UTF8') THEN
      RAISE EXCEPTION 'positive checkpoint ledger keys are not strictly sorted' USING ERRCODE='40001';
    END IF;
    last_positive_key:=positive_previous;
  END LOOP;
  SELECT COALESCE(jsonb_agg(to_jsonb(effect_instance_key)
      ORDER BY convert_to(effect_instance_key,'UTF8')),'[]'::jsonb)
    INTO db_inventory
    FROM (
      SELECT effect_instance_key FROM okg.external_action_proposals
       WHERE deployment_name=bound_deployment AND execution_epoch=attestation_row.old_epoch
      UNION
      SELECT d.effect_instance_key FROM okg.external_action_decisions d
       JOIN okg.external_action_proposals p ON p.deployment_name=d.deployment_name AND p.proposal_id=d.proposal_id
       WHERE d.deployment_name=bound_deployment AND p.execution_epoch=attestation_row.old_epoch
      UNION
      SELECT effect_instance_key FROM okg.external_action_executions
       WHERE deployment_name=bound_deployment AND execution_epoch=attestation_row.old_epoch
      UNION
      SELECT o.effect_instance_key FROM okg.external_action_outcomes o
       JOIN okg.external_action_executions e ON e.deployment_name=o.deployment_name AND e.execution_id=o.execution_id
       WHERE o.deployment_name=bound_deployment AND e.execution_epoch=attestation_row.old_epoch
      UNION
      SELECT effect_instance_key FROM okg.external_action_events
       WHERE deployment_name=bound_deployment AND execution_epoch=attestation_row.old_epoch
    ) database_inventory;
  IF db_inventory IS DISTINCT FROM inventory THEN
    RAISE EXCEPTION 'restore database inventory projection is not the retained old-epoch set' USING ERRCODE='40001';
  END IF;
  SELECT COALESCE(jsonb_agg(to_jsonb(effect_instance_key)
      ORDER BY convert_to(effect_instance_key,'UTF8')),'[]'::jsonb)
    INTO derived_inventory
    FROM (
      SELECT value #>> '{}' AS effect_instance_key
        FROM jsonb_array_elements(db_inventory) AS database_item(value)
      UNION
      SELECT e.effect_instance_key FROM okg.external_action_restore_checkpoint_entries e
       WHERE e.deployment_name=bound_deployment AND e.restore_id=session_row.restore_id
         AND e.checkpoint_payload->>'proof_kind'='positive'
    ) inventory_union;
  derived_inventory_count:=jsonb_array_length(derived_inventory);
  derived_inventory_digest:=okg.external_action_restore_inventory_digest(derived_inventory);
  IF derived_inventory_count<>attestation_row.effect_inventory_count
     OR derived_inventory_digest<>attestation_row.effect_inventory_digest
     OR count_value<>derived_inventory_count
     OR keys IS DISTINCT FROM derived_inventory THEN
    RAISE EXCEPTION 'restore inventory is not the database plus positive-ledger union' USING ERRCODE='40001';
  END IF;
  RETURN jsonb_build_object(
    'deployment',bound_deployment,'restore_id',session_row.restore_id,
    'restore_attestation_envelope_digest',attestation_row.envelope_digest,
    'effect_inventory_digest',attestation_row.effect_inventory_digest,
    'effect_inventory_count',attestation_row.effect_inventory_count,
    'worker_checkpoint_set_digest',attestation_row.worker_checkpoint_set_digest,
    'worker_checkpoint_count',attestation_row.worker_checkpoint_count,
    'pre_restore_event_root_digest',attestation_row.pre_restore_event_root_digest,
    'pre_restore_event_head_count',attestation_row.pre_restore_event_head_count
  );
END
$external_action_restore_assert_coverage$;

-- Final ACL convergence: internal external-action helpers are never a public
-- SQL surface.  The eight unrelated SECURITY DEFINER helpers are guarded
-- because this migration can run before the module that creates them.
DO $external_action_final_acl_internal$
BEGIN
  REVOKE ALL ON FUNCTION okg.external_action_session_deployment() FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_assert_deployment(TEXT) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_require_command(JSONB,TEXT[],TEXT[]) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_json_bytes(JSONB) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_hash(TEXT,JSONB) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_effect_key(JSONB) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_reject_mutation() FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_monotonic_nomos_updated_at() FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_nomos_authority_lock() FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_nomos_policy_bundle(TEXT,TEXT,TEXT) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_nomos_persist_decision(JSONB) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_record_auth_audit(JSONB) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_nomos_authority_snapshot(TEXT,TEXT,TEXT,TEXT,TEXT) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_startup_login_binding() FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_claim_owner_unavailability(TEXT) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_store_snapshot() FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_authorized_read(JSONB) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_store_resolve_principal(TEXT,TEXT) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_insert_dpop_replay_nonce(TEXT,TEXT,TEXT,TEXT,TEXT,TEXT,TEXT,TIMESTAMPTZ,TIMESTAMPTZ) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_store_startup(JSONB) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_store_insert_lease(JSONB) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_submit(JSONB) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_review_transition(JSONB) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_claim(JSONB) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_prepare_claim(JSONB) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_report_outcome(JSONB) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_owner_unavailability(JSONB) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_materialize_expiry(TEXT) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_restore_create(JSONB) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_restore_batch(JSONB) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_restore_prepare(JSONB) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_restore_finalize(JSONB) FROM PUBLIC;
  IF to_regprocedure('okg.external_action_restore_classification(jsonb,text)') IS NOT NULL THEN
    EXECUTE 'REVOKE ALL ON FUNCTION okg.external_action_restore_classification(JSONB,TEXT) FROM PUBLIC';
  END IF;
  IF to_regprocedure('okg.external_action_restore_build_plan(jsonb)') IS NOT NULL THEN
    EXECUTE 'REVOKE ALL ON FUNCTION okg.external_action_restore_build_plan(JSONB) FROM PUBLIC';
  END IF;
  IF to_regprocedure('okg.external_action_restore_inventory_digest(jsonb)') IS NOT NULL THEN
    EXECUTE 'REVOKE ALL ON FUNCTION okg.external_action_restore_inventory_digest(JSONB) FROM PUBLIC';
  END IF;
  IF to_regprocedure('okg.external_action_restore_inventory(text)') IS NOT NULL THEN
    EXECUTE 'REVOKE ALL ON FUNCTION okg.external_action_restore_inventory(TEXT) FROM PUBLIC';
  END IF;
  IF to_regprocedure('okg.external_action_restore_checkpoint_set_digest(text)') IS NOT NULL THEN
    EXECUTE 'REVOKE ALL ON FUNCTION okg.external_action_restore_checkpoint_set_digest(TEXT) FROM PUBLIC';
  END IF;
  IF to_regprocedure('okg.external_action_restore_event_root(text)') IS NOT NULL THEN
    EXECUTE 'REVOKE ALL ON FUNCTION okg.external_action_restore_event_root(TEXT) FROM PUBLIC';
  END IF;
  IF to_regprocedure('okg.external_action_restore_assert_coverage(jsonb)') IS NOT NULL THEN
    EXECUTE 'REVOKE ALL ON FUNCTION okg.external_action_restore_assert_coverage(JSONB) FROM PUBLIC';
  END IF;
  IF to_regprocedure('okg.external_action_restore_lifecycle_gate()') IS NOT NULL THEN
    EXECUTE 'REVOKE ALL ON FUNCTION okg.external_action_restore_lifecycle_gate() FROM PUBLIC';
  END IF;

  IF to_regprocedure('okg.mark_attribute_effective_integrity_dirty()') IS NOT NULL THEN
    EXECUTE 'REVOKE ALL ON FUNCTION okg.mark_attribute_effective_integrity_dirty() FROM PUBLIC';
  END IF;
  IF to_regprocedure('okg.mark_attribute_effective_integrity_dirty_rows()') IS NOT NULL THEN
    EXECUTE 'REVOKE ALL ON FUNCTION okg.mark_attribute_effective_integrity_dirty_rows() FROM PUBLIC';
  END IF;
  IF to_regprocedure('okg.mark_attribute_effective_integrity_dirty_update()') IS NOT NULL THEN
    EXECUTE 'REVOKE ALL ON FUNCTION okg.mark_attribute_effective_integrity_dirty_update() FROM PUBLIC';
  END IF;
  IF to_regprocedure('okg.require_chronos_checkout(text)') IS NOT NULL THEN
    EXECUTE 'REVOKE ALL ON FUNCTION okg.require_chronos_checkout(text) FROM PUBLIC';
  END IF;
  IF to_regprocedure('okg.require_graph_checkout(text)') IS NOT NULL THEN
    EXECUTE 'REVOKE ALL ON FUNCTION okg.require_graph_checkout(text) FROM PUBLIC';
  END IF;
  IF to_regprocedure('okg.runtime_queue_health(integer)') IS NOT NULL THEN
    EXECUTE 'REVOKE ALL ON FUNCTION okg.runtime_queue_health(integer) FROM PUBLIC';
  END IF;
  IF to_regprocedure('okg.source_frontier_active_workflows()') IS NOT NULL THEN
    EXECUTE 'REVOKE ALL ON FUNCTION okg.source_frontier_active_workflows() FROM PUBLIC';
  END IF;
  IF to_regprocedure('okg.source_frontier_state()') IS NOT NULL THEN
    EXECUTE 'REVOKE ALL ON FUNCTION okg.source_frontier_state() FROM PUBLIC';
  END IF;
END
$external_action_final_acl_internal$;

-- The coverage helper is defined after the historical grants above.  Keep it
-- owner-only just like the other restore planning/validation helpers; the API
-- can reach it only through prepare/finalize.
DO $external_action_restore_coverage_acl$
BEGIN
  REVOKE ALL ON FUNCTION okg.external_action_restore_assert_coverage(JSONB) FROM PUBLIC;
  IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname='okg_external_action_function_owner') THEN
    ALTER FUNCTION okg.external_action_restore_assert_coverage(JSONB) OWNER TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_assert_coverage(JSONB) TO okg_external_action_function_owner;
  END IF;
END
$external_action_restore_coverage_acl$;

CREATE OR REPLACE FUNCTION okg.external_action_restore_create(p_command JSONB)
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,okg
AS $external_action_restore_create_hardened$
DECLARE bound_deployment TEXT:=okg.external_action_session_deployment();
  session_row okg.external_action_restore_sessions%ROWTYPE;
  attestation_row okg.external_action_restore_attestations%ROWTYPE;
  fence_row okg.external_action_restore_fences%ROWTYPE;
  old_lease_row okg.external_action_execution_epoch_leases%ROWTYPE;
  new_lease_row okg.external_action_execution_epoch_leases%ROWTYPE;
  auth_row okg.external_action_auth_audits%ROWTYPE;
  completion_row okg.external_action_restore_completion_receipts%ROWTYPE;
  existing_attestation okg.external_action_restore_attestations%ROWTYPE;
  inventory_digest TEXT; expected_request_hash TEXT; evidence JSONB; evidence_item JSONB;
  evidence_name TEXT;
  observed_at TIMESTAMPTZ:=clock_timestamp();
BEGIN
  PERFORM okg.external_action_verifier_capability_status();
  PERFORM okg.external_action_require_command(
    p_command,
    ARRAY['deployment','restore_id','old_epoch','new_epoch','old_epoch_lease_digest',
      'new_epoch_lease_digest','ledger_fence_envelope_digest','fenced_ledger_root_digest',
      'fenced_ledger_entry_count','restored_snapshot_digest','pre_restore_event_root_digest',
      'pre_restore_event_head_count','effect_inventory','effect_inventory_digest',
      'effect_inventory_count','worker_checkpoint_set_digest','worker_checkpoint_count',
      'issued_at','expires_at','nonce','signer_separation_subject','protected_b64',
      'payload_b64','signature_b64','signer_key_id_digest','envelope_digest',
      'authentication_audit_id','nomos_decision_id','operator_separation_subject',
      'request_hash','idempotency_key_hash','old_epoch_lease_envelope_digest',
      'new_epoch_lease_envelope_digest','worker_quiescence_envelope_digest',
      'provider_authority_fence_envelope_digest','restore_evidence'],
    ARRAY['deployment','restore_id','old_epoch','new_epoch','old_epoch_lease_digest',
      'new_epoch_lease_digest','ledger_fence_envelope_digest','fenced_ledger_root_digest',
      'fenced_ledger_entry_count','restored_snapshot_digest','pre_restore_event_root_digest',
      'pre_restore_event_head_count','effect_inventory','effect_inventory_digest',
      'effect_inventory_count','worker_checkpoint_set_digest','worker_checkpoint_count',
      'issued_at','expires_at','nonce','signer_separation_subject','protected_b64',
      'payload_b64','signature_b64','signer_key_id_digest','envelope_digest',
      'authentication_audit_id','nomos_decision_id','operator_separation_subject',
      'request_hash','idempotency_key_hash','old_epoch_lease_envelope_digest',
      'new_epoch_lease_envelope_digest','worker_quiescence_envelope_digest',
      'provider_authority_fence_envelope_digest','restore_evidence']
  );
  IF p_command->>'deployment' IS DISTINCT FROM bound_deployment THEN
    RAISE EXCEPTION 'external action deployment is not bound to session_user' USING ERRCODE='28000';
  END IF;
  IF (p_command->>'new_epoch')::bigint <= (p_command->>'old_epoch')::bigint THEN
    RAISE EXCEPTION 'restore epoch must advance' USING ERRCODE='40001';
  END IF;
  inventory_digest:=okg.external_action_restore_inventory_digest(p_command->'effect_inventory');
  -- `effect_inventory` is the authoritative old-epoch database base returned
  -- by the inventory RPC.  Its digest/count are intentionally different
  -- from the signed attestation's final base UNION positive-ledger inventory;
  -- the latter is checked only after all checkpoint leaves are staged.
  -- The service has already verified the six detached envelopes.  SQL does
  -- not reimplement cryptography, but it does bind every retained evidence
  -- projection to the digest that was verified and to the attestation fields
  -- persisted below; an unbound evidence object cannot authorize restore.
  evidence:=p_command->'restore_evidence';
  IF jsonb_typeof(evidence) IS DISTINCT FROM 'object'
     OR (SELECT count(*) FROM jsonb_object_keys(evidence))<>6
     OR EXISTS (SELECT 1 FROM jsonb_object_keys(evidence) name
       WHERE name NOT IN ('old_epoch_lease','new_epoch_lease','worker_quiescence',
                          'provider_authority_fence','ledger_fence','restore_attestation')) THEN
    RAISE EXCEPTION 'restore evidence does not contain the closed six-envelope set' USING ERRCODE='22023';
  END IF;
  FOR evidence_name IN SELECT jsonb_object_keys(evidence) LOOP
    evidence_item:=evidence->evidence_name;
    IF jsonb_typeof(evidence_item) IS DISTINCT FROM 'object'
       OR EXISTS (SELECT 1 FROM jsonb_object_keys(evidence_item) field
          WHERE field NOT IN ('envelope','payload','envelope_digest'))
       OR NOT (evidence_item ? 'envelope') OR NOT (evidence_item ? 'payload')
       OR NOT (evidence_item ? 'envelope_digest')
       OR jsonb_typeof(evidence_item->'envelope') IS DISTINCT FROM 'object'
       OR jsonb_typeof(evidence_item->'payload') IS DISTINCT FROM 'object' THEN
      RAISE EXCEPTION 'restore evidence item is not a closed verified projection' USING ERRCODE='22023';
    END IF;
  END LOOP;
  IF evidence->'old_epoch_lease'->>'envelope_digest' IS DISTINCT FROM p_command->>'old_epoch_lease_envelope_digest'
     OR evidence->'new_epoch_lease'->>'envelope_digest' IS DISTINCT FROM p_command->>'new_epoch_lease_envelope_digest'
     OR evidence->'worker_quiescence'->>'envelope_digest' IS DISTINCT FROM p_command->>'worker_quiescence_envelope_digest'
     OR evidence->'provider_authority_fence'->>'envelope_digest' IS DISTINCT FROM p_command->>'provider_authority_fence_envelope_digest'
     OR evidence->'ledger_fence'->>'envelope_digest' IS DISTINCT FROM p_command->>'ledger_fence_envelope_digest'
     OR evidence->'restore_attestation'->>'envelope_digest' IS DISTINCT FROM p_command->>'envelope_digest' THEN
    RAISE EXCEPTION 'restore evidence envelope digest is not bound to the command' USING ERRCODE='40001';
  END IF;
  IF evidence->'restore_attestation'->'payload'->>'deployment' IS DISTINCT FROM p_command->>'deployment'
     OR evidence->'restore_attestation'->'payload'->>'restore_id' IS DISTINCT FROM p_command->>'restore_id'
     OR evidence->'restore_attestation'->'payload'->>'old_epoch' IS DISTINCT FROM p_command->>'old_epoch'
     OR evidence->'restore_attestation'->'payload'->>'new_epoch' IS DISTINCT FROM p_command->>'new_epoch'
     OR evidence->'restore_attestation'->'payload'->>'old_epoch_lease_digest' IS DISTINCT FROM p_command->>'old_epoch_lease_digest'
     OR evidence->'restore_attestation'->'payload'->>'new_epoch_lease_digest' IS DISTINCT FROM p_command->>'new_epoch_lease_digest'
     OR evidence->'restore_attestation'->'payload'->>'ledger_fence_envelope_digest' IS DISTINCT FROM p_command->>'ledger_fence_envelope_digest'
     OR evidence->'restore_attestation'->'payload'->>'fenced_ledger_root_digest' IS DISTINCT FROM p_command->>'fenced_ledger_root_digest'
     OR evidence->'restore_attestation'->'payload'->>'fenced_ledger_entry_count' IS DISTINCT FROM p_command->>'fenced_ledger_entry_count'
     OR evidence->'restore_attestation'->'payload'->>'restored_snapshot_digest' IS DISTINCT FROM p_command->>'restored_snapshot_digest'
     OR evidence->'restore_attestation'->'payload'->>'pre_restore_event_root_digest' IS DISTINCT FROM p_command->>'pre_restore_event_root_digest'
     OR evidence->'restore_attestation'->'payload'->>'pre_restore_event_head_count' IS DISTINCT FROM p_command->>'pre_restore_event_head_count'
     OR evidence->'restore_attestation'->'payload'->>'effect_inventory_digest' IS DISTINCT FROM p_command->>'effect_inventory_digest'
     OR evidence->'restore_attestation'->'payload'->>'effect_inventory_count' IS DISTINCT FROM p_command->>'effect_inventory_count'
     OR evidence->'restore_attestation'->'payload'->>'worker_checkpoint_set_digest' IS DISTINCT FROM p_command->>'worker_checkpoint_set_digest'
     OR evidence->'restore_attestation'->'payload'->>'worker_checkpoint_count' IS DISTINCT FROM p_command->>'worker_checkpoint_count'
     OR evidence->'restore_attestation'->'payload'->>'issued_at' IS DISTINCT FROM p_command->>'issued_at'
     OR evidence->'restore_attestation'->'payload'->>'expires_at' IS DISTINCT FROM p_command->>'expires_at'
     OR evidence->'restore_attestation'->'payload'->>'nonce' IS DISTINCT FROM p_command->>'nonce'
     OR evidence->'restore_attestation'->'payload'->>'signer_separation_subject' IS DISTINCT FROM p_command->>'signer_separation_subject' THEN
    RAISE EXCEPTION 'restore attestation projection is not bound to the command' USING ERRCODE='40001';
  END IF;
  expected_request_hash:=okg.external_action_hash(
    'okg-external-action-restore-create-request-v1',jsonb_build_object(
      'restore_id',p_command->>'restore_id',
      'old_epoch_lease_envelope_digest',p_command->>'old_epoch_lease_envelope_digest',
      'new_epoch_lease_envelope_digest',p_command->>'new_epoch_lease_envelope_digest',
      'worker_quiescence_envelope_digest',p_command->>'worker_quiescence_envelope_digest',
      'provider_authority_fence_envelope_digest',p_command->>'provider_authority_fence_envelope_digest',
      'ledger_fence_envelope_digest',p_command->>'ledger_fence_envelope_digest',
      'restore_attestation_envelope_digest',p_command->>'envelope_digest'));
  IF p_command->>'request_hash' IS DISTINCT FROM expected_request_hash THEN
    RAISE EXCEPTION 'restore create request hash is not canonical' USING ERRCODE='40001';
  END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended('okg.external_action.deployment:'||bound_deployment,0));
  PERFORM okg.external_action_nomos_authority_snapshot(bound_deployment,
    p_command->>'authentication_audit_id',p_command->>'nomos_decision_id','external_action.restore_reconcile',p_command->>'request_hash');
  SELECT * INTO auth_row FROM okg.external_action_auth_audits
   WHERE deployment_name=bound_deployment AND authentication_audit_id=p_command->>'authentication_audit_id';
  IF auth_row.separation_subject IS DISTINCT FROM p_command->>'operator_separation_subject'
     OR p_command->>'signer_separation_subject' IS DISTINCT FROM p_command->>'operator_separation_subject' THEN
    RAISE EXCEPTION 'restore operator and signed attestation subject are not bound' USING ERRCODE='42501';
  END IF;
  -- An exact create replay is allowed after finalization, when the normal
  -- fence is already back in service.  Return the durable completion receipt
  -- (or the immutable staging projection) instead of reporting the old
  -- mutable session status or attempting to create a second attestation.
  SELECT * INTO session_row FROM okg.external_action_restore_sessions
   WHERE deployment_name=bound_deployment AND restore_id=p_command->>'restore_id' FOR UPDATE;
  IF session_row.restore_session_row_id IS NOT NULL THEN
    IF session_row.request_hash IS DISTINCT FROM p_command->>'request_hash'
       OR session_row.idempotency_key_hash IS DISTINCT FROM p_command->>'idempotency_key_hash'
       OR session_row.operator_separation_subject IS DISTINCT FROM p_command->>'operator_separation_subject' THEN
      RAISE EXCEPTION 'restore id was reused with a different operator, key, or request' USING ERRCODE='40001';
    END IF;
    SELECT * INTO completion_row FROM okg.external_action_restore_completion_receipts
     WHERE deployment_name=bound_deployment AND restore_id=session_row.restore_id FOR SHARE;
    IF completion_row.completion_receipt_row_id IS NOT NULL THEN
      IF jsonb_typeof(completion_row.store_record) = 'object' THEN
        RETURN completion_row.store_record || jsonb_build_object('replay',TRUE);
      END IF;
      RETURN jsonb_build_object('replay',TRUE,'deployment',bound_deployment,
        'restore_id',session_row.restore_id,'state','complete',
        'quarantine_state','cleared','service_mode','normal',
        'restore_attestation_envelope_digest',session_row.restore_attestation_envelope_digest);
    END IF;
    SELECT * INTO attestation_row FROM okg.external_action_restore_attestations
     WHERE deployment_name=bound_deployment AND envelope_digest=session_row.restore_attestation_envelope_digest FOR SHARE;
    RETURN jsonb_build_object('replay',TRUE,'deployment',bound_deployment,
      'restore_id',session_row.restore_id,'state','collecting_checkpoints',
      'quarantine_state','blocked',
      'service_mode','restore_only',
      'restore_attestation_envelope_digest',session_row.restore_attestation_envelope_digest,
      'effect_inventory_digest',attestation_row.effect_inventory_digest,
      'effect_inventory_count',attestation_row.effect_inventory_count,
      'expected_checkpoint_set_digest',attestation_row.worker_checkpoint_set_digest,
      'expected_checkpoint_count',attestation_row.worker_checkpoint_count,
      'accepted_checkpoint_count',(
        SELECT count(*) FROM okg.external_action_restore_checkpoint_entries e
         WHERE e.deployment_name=bound_deployment AND e.restore_id=session_row.restore_id));
  END IF;
  SELECT * INTO fence_row FROM okg.external_action_restore_fences
   WHERE deployment_name=bound_deployment FOR UPDATE;
  IF fence_row.deployment_name IS NULL THEN
    RAISE EXCEPTION 'persisted startup restore fence is unavailable' USING ERRCODE='50300';
  END IF;
  SELECT * INTO old_lease_row
    FROM okg.external_action_execution_epoch_leases
   WHERE deployment_name=bound_deployment AND epoch=(p_command->>'old_epoch')::bigint
   ORDER BY lease_sequence DESC LIMIT 1;
  SELECT * INTO new_lease_row
    FROM okg.external_action_execution_epoch_leases
   WHERE deployment_name=bound_deployment AND epoch=(p_command->>'new_epoch')::bigint
   ORDER BY lease_sequence DESC LIMIT 1;
  IF old_lease_row.lease_row_id IS NULL
     OR new_lease_row.lease_row_id IS NULL
     OR old_lease_row.envelope_digest IS DISTINCT FROM p_command->>'old_epoch_lease_envelope_digest'
     OR new_lease_row.envelope_digest IS DISTINCT FROM p_command->>'new_epoch_lease_envelope_digest'
     OR old_lease_row.envelope_digest IS DISTINCT FROM p_command->>'old_epoch_lease_digest'
     OR new_lease_row.envelope_digest IS DISTINCT FROM p_command->>'new_epoch_lease_digest'
     OR fence_row.highest_accepted_epoch IS DISTINCT FROM new_lease_row.epoch
     OR fence_row.lease_digest IS DISTINCT FROM new_lease_row.envelope_digest
     OR fence_row.lease_chain_digest IS DISTINCT FROM new_lease_row.lease_chain_digest
     OR fence_row.verification_config_digest IS DISTINCT FROM new_lease_row.verification_config_digest
     OR fence_row.verification_login_binding_digest IS DISTINCT FROM new_lease_row.verification_login_binding_digest
     OR fence_row.service_mode <> 'restore_only'
     OR fence_row.quarantine_state <> 'blocked' THEN
    RAISE EXCEPTION 'restore epochs are not bound to the installed quarantined lease fence'
      USING ERRCODE='40001';
  END IF;
  SELECT * INTO existing_attestation FROM okg.external_action_restore_attestations
   WHERE deployment_name=bound_deployment AND envelope_digest=p_command->>'envelope_digest' FOR SHARE;
  IF existing_attestation.attestation_row_id IS NOT NULL THEN
    RAISE EXCEPTION 'restore attestation envelope digest was already used' USING ERRCODE='40001';
  END IF;
  INSERT INTO okg.external_action_restore_attestations (
    deployment_name,restore_id,old_epoch,new_epoch,old_epoch_lease_digest,new_epoch_lease_digest,
    ledger_fence_envelope_digest,fenced_ledger_root_digest,fenced_ledger_entry_count,
    restored_snapshot_digest,pre_restore_event_root_digest,pre_restore_event_head_count,
    effect_inventory_digest,effect_inventory_count,worker_checkpoint_set_digest,
    worker_checkpoint_count,issued_at,expires_at,nonce,signer_separation_subject,
    protected_b64,payload_b64,signature_b64,signer_key_id_digest,envelope_digest,
    verified_at,authentication_audit_id,nomos_decision_id,store_record
  ) VALUES (
    bound_deployment,p_command->>'restore_id',(p_command->>'old_epoch')::bigint,
    (p_command->>'new_epoch')::bigint,p_command->>'old_epoch_lease_digest',
    p_command->>'new_epoch_lease_digest',p_command->>'ledger_fence_envelope_digest',
    p_command->>'fenced_ledger_root_digest',(p_command->>'fenced_ledger_entry_count')::bigint,
    p_command->>'restored_snapshot_digest',p_command->>'pre_restore_event_root_digest',
    (p_command->>'pre_restore_event_head_count')::bigint,p_command->>'effect_inventory_digest',
    (p_command->>'effect_inventory_count')::bigint,p_command->>'worker_checkpoint_set_digest',
    (p_command->>'worker_checkpoint_count')::bigint,(p_command->>'issued_at')::timestamptz,
    (p_command->>'expires_at')::timestamptz,p_command->>'nonce',p_command->>'signer_separation_subject',
    p_command->>'protected_b64',p_command->>'payload_b64',p_command->>'signature_b64',
    p_command->>'signer_key_id_digest',p_command->>'envelope_digest',observed_at,
    p_command->>'authentication_audit_id',p_command->>'nomos_decision_id',p_command
  ) RETURNING * INTO attestation_row;
  INSERT INTO okg.external_action_restore_sessions (
    deployment_name,restore_id,request_hash,idempotency_key_hash,operator_separation_subject,
    authentication_audit_id,nomos_decision_id,restore_attestation_envelope_digest,
    status,created_at,updated_at,store_record,effect_inventory,effect_inventory_digest,
    effect_inventory_count,old_epoch_lease_envelope_digest,new_epoch_lease_envelope_digest,
    worker_quiescence_envelope_digest,provider_authority_fence_envelope_digest,
    ledger_fence_envelope_digest
  ) VALUES (
    bound_deployment,p_command->>'restore_id',p_command->>'request_hash',
    p_command->>'idempotency_key_hash',p_command->>'operator_separation_subject',
    p_command->>'authentication_audit_id',p_command->>'nomos_decision_id',attestation_row.envelope_digest,
    'staging',observed_at,observed_at,p_command,p_command->'effect_inventory',
    -- The session retains the deployment-owned database base inventory.  The
    -- signed attestation digest/count describe that base UNION the complete
    -- positive checkpoint leaf set and therefore cannot be used as the base
    -- projection's digest before checkpoint staging is complete.
    inventory_digest,jsonb_array_length(p_command->'effect_inventory'),
    COALESCE(p_command->>'old_epoch_lease_envelope_digest',p_command->>'old_epoch_lease_digest'),
    COALESCE(p_command->>'new_epoch_lease_envelope_digest',p_command->>'new_epoch_lease_digest'),
    p_command->>'worker_quiescence_envelope_digest',p_command->>'provider_authority_fence_envelope_digest',
    p_command->>'ledger_fence_envelope_digest'
  ) RETURNING * INTO session_row;
  UPDATE okg.external_action_restore_fences
     SET quarantine_state='blocked',service_mode='restore_only',
         restore_attestation_digest=attestation_row.envelope_digest,mode_changed_at=observed_at
   WHERE deployment_name=bound_deployment;
  RETURN jsonb_build_object('replay',FALSE,'deployment',bound_deployment,
    'restore_id',session_row.restore_id,'state','collecting_checkpoints',
    'quarantine_state','blocked','service_mode','restore_only',
    'restore_attestation_envelope_digest',session_row.restore_attestation_envelope_digest,
    'effect_inventory_digest',attestation_row.effect_inventory_digest,
    'effect_inventory_count',attestation_row.effect_inventory_count,
    'expected_checkpoint_set_digest',attestation_row.worker_checkpoint_set_digest,
    'expected_checkpoint_count',attestation_row.worker_checkpoint_count,
    'accepted_checkpoint_count',0);
END
$external_action_restore_create_hardened$;

DO $external_action_restore_hardened_grants$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname='okg_external_action_api') THEN
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_prepare(JSONB) TO okg_external_action_api;
  END IF;
  IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname='okg_external_action_function_owner') THEN
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_inventory_digest(JSONB) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_checkpoint_set_digest(TEXT) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_event_root(TEXT) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_assert_coverage(JSONB) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_prepare(JSONB) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_lifecycle_gate() TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_restore_inventory_digest(JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_restore_checkpoint_set_digest(TEXT) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_restore_event_root(TEXT) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_restore_assert_coverage(JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_restore_prepare(JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_restore_lifecycle_gate() OWNER TO okg_external_action_function_owner;
  END IF;
END
$external_action_restore_hardened_grants$;

DO $external_action_restore_hardened_acl$
BEGIN
  REVOKE ALL ON FUNCTION okg.external_action_restore_prepare(JSONB) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_restore_inventory_digest(JSONB) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_restore_checkpoint_set_digest(TEXT) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_restore_event_root(TEXT) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_restore_assert_coverage(JSONB) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_restore_lifecycle_gate() FROM PUBLIC;
  IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname='okg_external_action_api') THEN
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_prepare(JSONB) TO okg_external_action_api;
  END IF;
  IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname='okg_external_action_function_owner') THEN
    ALTER FUNCTION okg.external_action_restore_prepare(JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_restore_inventory_digest(JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_restore_checkpoint_set_digest(TEXT) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_restore_event_root(TEXT) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_restore_assert_coverage(JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_restore_lifecycle_gate() OWNER TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_prepare(JSONB) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_inventory_digest(JSONB) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_checkpoint_set_digest(TEXT) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_event_root(TEXT) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_assert_coverage(JSONB) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_lifecycle_gate() TO okg_external_action_function_owner;
  END IF;
END
$external_action_restore_hardened_acl$;

-- Derive the recovery class from the verified worker journal phase and the
-- proof kind.  `classification` is an internal projection and is never
-- accepted as authority from the worker payload.
CREATE OR REPLACE FUNCTION okg.external_action_restore_classification(
  p_payload JSONB, p_proof_kind TEXT
) RETURNS TEXT
LANGUAGE plpgsql IMMUTABLE SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $external_action_restore_classification$
DECLARE phase_value TEXT; outcome_value TEXT;
BEGIN
  IF jsonb_typeof(p_payload) IS DISTINCT FROM 'object'
     OR p_proof_kind NOT IN ('positive','nonmembership') THEN
    RAISE EXCEPTION 'restore checkpoint projection is invalid' USING ERRCODE='22023';
  END IF;
  phase_value:=p_payload->>'journal_phase';
  IF phase_value IN ('database_only_not_started','claimed_before_ledger_begin') THEN
    IF p_proof_kind<>'nonmembership'
       OR p_payload->>'positive_proof_digest' IS NOT NULL
       OR p_payload->>'ledger_entry_digest' IS NOT NULL
       OR p_payload->>'provider_idempotency_key_digest' IS NOT NULL
       OR p_payload->>'effect_started_at' IS NOT NULL
       OR p_payload->>'provider_receipt_digest' IS NOT NULL
       OR p_payload->>'outcome' IS NOT NULL
       OR p_payload->>'outcome_reason_code' IS NOT NULL
       OR p_payload->>'outcome_recorded_at' IS NOT NULL
       OR p_payload->>'journal_digest' IS NOT NULL THEN
      RAISE EXCEPTION 'database-only checkpoint has illegal ledger fields' USING ERRCODE='40001';
    END IF;
    RETURN 'database_restore_no_effect_started';
  END IF;
  IF phase_value NOT IN ('effect_starting','provider_receipt_observed','outcome_reported')
     OR p_proof_kind<>'positive'
     OR p_payload->>'nonmembership_proof_digest' IS NOT NULL
     OR p_payload->>'ledger_entry_digest' IS NULL
     OR p_payload->>'provider_idempotency_key_digest' IS NULL
     OR p_payload->>'effect_started_at' IS NULL
     OR p_payload->>'journal_digest' IS NULL THEN
    RAISE EXCEPTION 'restore checkpoint phase and proof combination is invalid' USING ERRCODE='40001';
  END IF;
  IF phase_value='effect_starting' THEN
    IF p_payload->>'provider_receipt_digest' IS NOT NULL OR p_payload->>'outcome' IS NOT NULL
       OR p_payload->>'outcome_reason_code' IS NOT NULL THEN
      RAISE EXCEPTION 'effect-starting checkpoint has terminal fields' USING ERRCODE='40001';
    END IF;
    RETURN 'database_restore_effect_uncertain';
  ELSIF phase_value='provider_receipt_observed' THEN
    IF p_payload->>'provider_receipt_digest' IS NULL OR p_payload->>'outcome' IS NOT NULL
       OR p_payload->>'outcome_reason_code' IS NOT NULL THEN
      RAISE EXCEPTION 'provider-receipt checkpoint has illegal outcome fields' USING ERRCODE='40001';
    END IF;
    -- PACT treats an authenticated provider receipt as authoritative
    -- completion.  Build-plan/finalize binds the reason to the retained
    -- action contract before persisting the succeeded outcome.
    RETURN 'database_restore_succeeded';
  END IF;
  outcome_value:=p_payload->>'outcome';
  IF outcome_value NOT IN ('succeeded','failed','indeterminate')
     OR p_payload->>'outcome_reason_code' IS NULL
     OR p_payload->>'outcome_recorded_at' IS NULL THEN
    RAISE EXCEPTION 'outcome checkpoint is invalid' USING ERRCODE='40001';
  END IF;
  IF outcome_value='succeeded' THEN
    IF p_payload->>'provider_receipt_digest' IS NULL THEN
      RAISE EXCEPTION 'successful checkpoint has no provider receipt' USING ERRCODE='40001';
    END IF;
    RETURN 'database_restore_succeeded';
  ELSIF outcome_value='failed' THEN
    -- Whether a failed provider outcome requires or forbids a receipt is an
    -- action-contract rule.  Build-plan revalidates that retained rule after
    -- resolving the proposal; the classifier must not reject a valid
    -- provider-declared failed outcome merely because it has a receipt.
    RETURN 'database_restore_failed';
  END IF;
  IF p_payload->>'provider_receipt_digest' IS NOT NULL THEN
    RAISE EXCEPTION 'indeterminate checkpoint has a provider receipt' USING ERRCODE='40001';
  END IF;
  RETURN 'database_restore_effect_uncertain';
END
$external_action_restore_classification$;

-- Build, but do not persist, the complete recovery/event plan.  Prepare and
-- finalize run on the same database transaction; the signed completion
-- receipt therefore covers this exact timestamp and these exact hashes.
CREATE OR REPLACE FUNCTION okg.external_action_restore_build_plan(p_command JSONB)
RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $external_action_restore_build_plan$
DECLARE
  bound_deployment TEXT:=okg.external_action_session_deployment();
  session_row okg.external_action_restore_sessions%ROWTYPE;
  attestation_row okg.external_action_restore_attestations%ROWTYPE;
  proposal_row okg.external_action_proposals%ROWTYPE;
  decision_row okg.external_action_decisions%ROWTYPE;
  execution_row okg.external_action_executions%ROWTYPE;
  outcome_row okg.external_action_outcomes%ROWTYPE;
  payload_row okg.external_action_payload_versions%ROWTYPE;
  entry RECORD; payload JSONB; checked JSONB;
  recovery_projection JSONB; event_projection JSONB; recovery_items JSONB:='[]'::jsonb;
  event_items JSONB:='[]'::jsonb; recovery_digests JSONB:='[]'::jsonb;
  classification_counts JSONB:='{}'::jsonb;
  pre_root JSONB; predicted_heads JSONB; new_head JSONB; post_root JSONB;
  recovery_set JSONB; coverage_command JSONB;
  source_proposal TEXT; source_decision TEXT; source_execution TEXT;
  classification TEXT; event_outcome TEXT; event_reason TEXT;
  recovered_outcome_value TEXT;
  before_review TEXT; after_review TEXT; before_execution TEXT; after_execution TEXT;
  submission_hash TEXT; previous_hash TEXT; event_hash_value TEXT;
  recovery_digest_value TEXT; previous_sequence BIGINT; event_sequence BIGINT;
  lifecycle_value BIGINT; payload_version_value BIGINT;
  effective_value TIMESTAMPTZ; completed_value TIMESTAMPTZ;
  actor_principal_value TEXT; policy_digest_value TEXT;
  decision_binding_value TEXT; execution_binding_value TEXT; outcome_request_value TEXT;
  projection_changed BOOLEAN; existing_outcome_value TEXT; contract_rule JSONB;
BEGIN
  PERFORM okg.external_action_require_command(
    p_command,
    ARRAY['deployment','restore_id','restore_attestation_envelope_digest',
      'expected_inventory_digest','expected_inventory_count',
      'expected_checkpoint_set_digest','expected_checkpoint_count',
      'completed_at','operator_separation_subject','idempotency_key_hash',
      'request_hash','authentication_audit_id','nomos_decision_id'],
    ARRAY['deployment','restore_id','restore_attestation_envelope_digest',
      'expected_inventory_digest','expected_inventory_count',
      'expected_checkpoint_set_digest','expected_checkpoint_count']
  );
  IF p_command->>'deployment' IS DISTINCT FROM bound_deployment THEN
    RAISE EXCEPTION 'external action deployment is not bound to session_user' USING ERRCODE='28000';
  END IF;
  coverage_command:=p_command-ARRAY['completed_at','operator_separation_subject','idempotency_key_hash','request_hash','authentication_audit_id','nomos_decision_id'];
  checked:=okg.external_action_restore_assert_coverage(coverage_command);
  SELECT * INTO session_row FROM okg.external_action_restore_sessions
    WHERE deployment_name=bound_deployment AND restore_id=p_command->>'restore_id' FOR UPDATE;
  SELECT * INTO attestation_row FROM okg.external_action_restore_attestations
    WHERE deployment_name=bound_deployment AND envelope_digest=session_row.restore_attestation_envelope_digest FOR SHARE;
  IF session_row.restore_session_row_id IS NULL OR attestation_row.attestation_row_id IS NULL THEN
    RAISE EXCEPTION 'restore plan has no immutable session or attestation' USING ERRCODE='40001';
  END IF;
  IF p_command ? 'authentication_audit_id' AND p_command->>'authentication_audit_id' IS DISTINCT FROM session_row.authentication_audit_id THEN
    RAISE EXCEPTION 'restore plan authentication audit is not session-bound' USING ERRCODE='42501';
  END IF;
  IF p_command ? 'nomos_decision_id' AND p_command->>'nomos_decision_id' IS DISTINCT FROM session_row.nomos_decision_id THEN
    RAISE EXCEPTION 'restore plan Nomos decision is not session-bound' USING ERRCODE='42501';
  END IF;
  IF clock_timestamp() >= attestation_row.expires_at THEN
    RAISE EXCEPTION 'restore attestation has expired' USING ERRCODE='40001';
  END IF;
  SELECT a.principal INTO actor_principal_value FROM okg.external_action_auth_audits a
   WHERE a.deployment_name=bound_deployment AND a.authentication_audit_id=session_row.authentication_audit_id
     AND a.separation_subject=session_row.operator_separation_subject;
  SELECT n.policy_fingerprint INTO policy_digest_value FROM okg.nomos_decisions n
   WHERE n.deployment_name=bound_deployment AND n.decision_id=session_row.nomos_decision_id;
  IF actor_principal_value IS NULL OR policy_digest_value IS NULL THEN
    RAISE EXCEPTION 'restore plan authentication or policy projection is unavailable' USING ERRCODE='42501';
  END IF;
  completed_value:=COALESCE(NULLIF(p_command->>'completed_at','')::timestamptz,clock_timestamp());
  pre_root:=okg.external_action_restore_event_root(bound_deployment);
  predicted_heads:=pre_root->'sorted_event_head_entries';
  FOR entry IN SELECT e.* FROM okg.external_action_restore_checkpoint_entries e
    WHERE e.deployment_name=bound_deployment AND e.restore_id=session_row.restore_id
    ORDER BY convert_to(e.effect_instance_key,'UTF8') LOOP
    payload:=entry.checkpoint_payload;
    IF jsonb_typeof(entry.proof) IS DISTINCT FROM 'object'
       OR entry.proof_digest IS DISTINCT FROM okg.external_action_hash(
         CASE WHEN payload->>'proof_kind'='positive'
              THEN 'okg-external-action-restore-positive-proof-v1'
              ELSE 'okg-external-action-restore-nonmembership-proof-v1' END,
         entry.proof) THEN
      RAISE EXCEPTION 'restore checkpoint proof digest is not bound to the retained verified proof'
        USING ERRCODE='40001';
    END IF;
    IF payload->>'proof_kind'='positive' THEN
      IF entry.proof->>'effect_instance_key' IS DISTINCT FROM entry.effect_instance_key
         OR entry.proof->>'ledger_entry_digest' IS DISTINCT FROM payload->>'ledger_entry_digest'
         OR entry.proof->>'leaf_count' IS DISTINCT FROM attestation_row.fenced_ledger_entry_count::text
         OR entry.leaf_index IS NULL
         OR entry.proof->>'leaf_index' IS DISTINCT FROM entry.leaf_index::text THEN
        RAISE EXCEPTION 'positive restore proof is not bound to the checkpoint ledger projection'
          USING ERRCODE='40001';
      END IF;
    ELSIF payload->>'proof_kind'='nonmembership' THEN
      IF entry.proof->>'effect_instance_key' IS DISTINCT FROM entry.effect_instance_key
         OR entry.proof->>'leaf_count' IS DISTINCT FROM attestation_row.fenced_ledger_entry_count::text THEN
        RAISE EXCEPTION 'nonmembership restore proof is not bound to the checkpoint ledger projection'
          USING ERRCODE='40001';
      END IF;
    END IF;
    classification:=okg.external_action_restore_classification(payload,payload->>'proof_kind');
    source_proposal:=NULLIF(payload->>'proposal_id','');
    source_decision:=NULLIF(payload->>'decision_id','');
    source_execution:=NULLIF(payload->>'execution_id','');
    -- A retained source row must carry its complete proposal/decision/claim
    -- chain.  When the restored snapshot did not retain those rows, a
    -- positive ledger leaf is still valid evidence for an orphan recovery
    -- tombstone; it must not be forced to invent source identifiers.
    IF (payload->>'proof_kind'='positive'
        OR payload->>'journal_phase'='claimed_before_ledger_begin')
       AND ((source_proposal IS NULL) <> (source_decision IS NULL)
            OR (source_decision IS NULL) <> (source_execution IS NULL)
            OR (source_proposal IS NOT NULL AND (
                 payload->>'proposal_request_hash' IS NULL
                 OR payload->>'original_payload_hash' IS NULL
                 OR payload->>'approved_payload_hash' IS NULL
                 OR payload->>'action_contract_digest' IS NULL
                 OR payload->>'decision_binding_hash' IS NULL
                 OR payload->>'execution_binding_hash' IS NULL
                 OR payload->>'okg_claim_handoff_receipt_digest' IS NULL
                 OR payload->>'pre_restore_effect_event_head_hash' IS NULL))) THEN
      RAISE EXCEPTION 'restore checkpoint has an incomplete OKG source chain' USING ERRCODE='40001';
    END IF;
    proposal_row:=NULL; decision_row:=NULL; execution_row:=NULL; outcome_row:=NULL;
    IF source_proposal IS NOT NULL THEN
      SELECT * INTO proposal_row FROM okg.external_action_proposals
       WHERE deployment_name=bound_deployment AND proposal_id=source_proposal FOR SHARE;
      IF proposal_row.proposal_id IS NULL OR proposal_row.effect_instance_key<>entry.effect_instance_key
         OR proposal_row.execution_epoch<>attestation_row.old_epoch THEN
        RAISE EXCEPTION 'restore checkpoint proposal binding is invalid' USING ERRCODE='40001';
      END IF;
      IF entry.effect_instance_key IS DISTINCT FROM okg.external_action_hash(
           'okg-external-action-effect-instance-v1',
           jsonb_build_object('deployment',bound_deployment,
             'proposer_separation_subject',proposal_row.proposer_separation_subject,
             'submission_idempotency_key_hash',proposal_row.submission_idempotency_key_hash))
         OR payload->>'submission_content_hash' IS DISTINCT FROM proposal_row.submission_content_hash
         OR payload->>'proposal_request_hash' IS DISTINCT FROM proposal_row.submission_request_hash
         OR payload->>'original_payload_hash' IS DISTINCT FROM proposal_row.original_payload_hash
         OR payload->>'action_contract_digest' IS DISTINCT FROM proposal_row.action_contract_digest
         OR payload->>'submission_content_hash' IS DISTINCT FROM okg.external_action_hash(
           'okg-external-action-submission-content-v1',jsonb_build_object(
             'automation_id',proposal_row.automation_id,'automation_version',proposal_row.automation_version,
             'run_id',proposal_row.run_id,'correlation_id',proposal_row.correlation_id,
             'action_type',proposal_row.action_type,'target_ref',proposal_row.target_ref,
             'risk_tier',proposal_row.risk_tier,'action_schema_digest',proposal_row.action_schema_digest,
             'action_contract_digest',proposal_row.action_contract_digest,
             'original_payload_hash',proposal_row.original_payload_hash,
             'requested_review_expires_at',to_char(proposal_row.requested_review_expires_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
             'summary_code',proposal_row.summary_code,'evidence_digests',proposal_row.evidence_digests)) THEN
        RAISE EXCEPTION 'restore checkpoint proposal semantic hash is invalid' USING ERRCODE='40001';
      END IF;
      SELECT * INTO payload_row FROM okg.external_action_payload_versions
       WHERE deployment_name=bound_deployment AND proposal_id=proposal_row.proposal_id
       ORDER BY payload_version ASC LIMIT 1 FOR SHARE;
      IF payload_row.payload_version_id IS NULL
         OR payload_row.payload_hash IS DISTINCT FROM proposal_row.original_payload_hash
         OR payload_row.payload_hash IS DISTINCT FROM okg.external_action_hash(
           'okg-external-action-payload-v1',convert_from(payload_row.canonical_payload,'UTF8')::jsonb) THEN
        RAISE EXCEPTION 'restore checkpoint original payload hash is invalid' USING ERRCODE='40001';
      END IF;
      IF payload->>'journal_phase'='outcome_reported' THEN
        contract_rule:=proposal_row.action_contract_record->'outcome_reason_rules'->(payload->>'outcome_reason_code');
        IF jsonb_typeof(proposal_row.action_contract_record) IS DISTINCT FROM 'object'
           OR jsonb_typeof(proposal_row.action_contract_record->'outcome_reason_rules') IS DISTINCT FROM 'object'
           OR jsonb_typeof(contract_rule) IS DISTINCT FROM 'object'
           OR jsonb_typeof(contract_rule->'allowed_outcomes') IS DISTINCT FROM 'array'
           OR NOT COALESCE(contract_rule->'allowed_outcomes' ? (payload->>'outcome'),FALSE)
           OR contract_rule->>'provider_receipt' IS NULL
           OR contract_rule->>'provider_receipt' NOT IN ('required','forbidden')
           OR ((contract_rule->>'provider_receipt')='required') IS DISTINCT FROM (payload->>'provider_receipt_digest' IS NOT NULL) THEN
          RAISE EXCEPTION 'restore checkpoint outcome is not allowed by the retained action contract' USING ERRCODE='40001';
        END IF;
      END IF;
    END IF;
    IF source_decision IS NOT NULL THEN
      IF source_proposal IS NULL THEN
        RAISE EXCEPTION 'restore checkpoint decision has no proposal' USING ERRCODE='40001';
      END IF;
      SELECT * INTO decision_row FROM okg.external_action_decisions
       WHERE deployment_name=bound_deployment AND decision_id=source_decision FOR SHARE;
      IF decision_row.decision_id IS NULL OR decision_row.proposal_id<>source_proposal
         OR decision_row.effect_instance_key<>entry.effect_instance_key
         OR decision_row.execution_epoch<>attestation_row.old_epoch THEN
        RAISE EXCEPTION 'restore checkpoint decision binding is invalid' USING ERRCODE='40001';
      END IF;
      decision_binding_value:=okg.external_action_hash(
        'okg-external-action-decision-binding-v1',jsonb_build_object(
          'deployment',bound_deployment,'execution_epoch',decision_row.execution_epoch,
          'effect_instance_key',decision_row.effect_instance_key,'proposal_id',decision_row.proposal_id,
          'decision_id',decision_row.decision_id,
          'approved_lifecycle_version',decision_row.approved_lifecycle_version,
          'approved_payload_version',decision_row.approved_payload_version,
          'approved_payload_hash',decision_row.approved_payload_hash,
          'action_type',decision_row.action_type,'target_ref',decision_row.target_ref,
          'action_schema_digest',decision_row.action_schema_digest,
          'action_contract_digest',decision_row.action_contract_digest,
          'executor_contract_digest',decision_row.executor_contract_digest,
          'payload_renderer_digest',decision_row.payload_renderer_digest,
          'governed_tool_binding_digest',decision_row.governed_tool_binding_digest,
          'provider_connection_contract_digest',decision_row.provider_connection_contract_digest,
          'provider_capability_digest',decision_row.provider_capability_digest,
          'provider_operation_id',decision_row.provider_operation_id,
          'provider_operation_descriptor_digest',decision_row.provider_operation_descriptor_digest,
          'policy_digest',decision_row.policy_digest,
          'execute_before',CASE WHEN decision_row.execute_before IS NULL THEN NULL ELSE to_char(decision_row.execute_before AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"') END));
      IF decision_row.decision_binding_hash IS DISTINCT FROM decision_binding_value
         OR payload->>'decision_binding_hash' IS DISTINCT FROM decision_binding_value
         OR payload->>'approved_payload_hash' IS DISTINCT FROM decision_row.approved_payload_hash THEN
        RAISE EXCEPTION 'restore checkpoint decision semantic hash is invalid' USING ERRCODE='40001';
      END IF;
    END IF;
    IF source_proposal IS NULL AND (
         payload->>'proposal_request_hash' IS NOT NULL
         OR payload->>'original_payload_hash' IS NOT NULL
         OR payload->>'action_contract_digest' IS NOT NULL
         OR payload->>'approved_payload_hash' IS NOT NULL) THEN
      RAISE EXCEPTION 'orphan restore checkpoint invents proposal fields' USING ERRCODE='40001';
    END IF;
    IF source_decision IS NULL AND (
         payload->>'decision_binding_hash' IS NOT NULL
         OR payload->>'approved_payload_hash' IS NOT NULL) THEN
      RAISE EXCEPTION 'restore checkpoint invents decision fields' USING ERRCODE='40001';
    END IF;
    IF source_execution IS NULL AND (
         payload->>'execution_binding_hash' IS NOT NULL
         OR payload->>'okg_claim_handoff_receipt_digest' IS NOT NULL
         OR payload->>'provider_idempotency_key_digest' IS NOT NULL) THEN
      RAISE EXCEPTION 'restore checkpoint invents execution fields' USING ERRCODE='40001';
    END IF;
    IF source_execution IS NOT NULL THEN
      IF source_proposal IS NULL OR source_decision IS NULL THEN
        RAISE EXCEPTION 'restore checkpoint execution has incomplete source chain' USING ERRCODE='40001';
      END IF;
      SELECT * INTO execution_row FROM okg.external_action_executions
       WHERE deployment_name=bound_deployment AND execution_id=source_execution FOR SHARE;
      IF execution_row.execution_id IS NULL OR execution_row.proposal_id<>source_proposal
         OR execution_row.decision_id<>source_decision
         OR execution_row.effect_instance_key<>entry.effect_instance_key
         OR execution_row.execution_epoch<>attestation_row.old_epoch THEN
        RAISE EXCEPTION 'restore checkpoint execution binding is invalid' USING ERRCODE='40001';
      END IF;
      execution_binding_value:=okg.external_action_hash(
        'okg-external-action-execution-binding-v1',jsonb_build_object(
          'deployment',bound_deployment,'execution_epoch',execution_row.execution_epoch,
          'execution_epoch_lease_digest',execution_row.execution_epoch_lease_digest,
          'effect_instance_key',execution_row.effect_instance_key,'proposal_id',execution_row.proposal_id,
          'decision_id',execution_row.decision_id,'execution_id',execution_row.execution_id,
          'worker_separation_subject',execution_row.worker_separation_subject,
          'approved_lifecycle_version',execution_row.approved_lifecycle_version,
          'approved_payload_version',execution_row.approved_payload_version,
          'approved_payload_hash',execution_row.approved_payload_hash,
          'action_type',execution_row.action_type,'target_ref',execution_row.target_ref,
          'action_schema_digest',execution_row.action_schema_digest,
          'action_contract_digest',execution_row.action_contract_digest,
          'executor_contract_digest',execution_row.executor_contract_digest,
          'payload_renderer_digest',execution_row.payload_renderer_digest,
          'governed_tool_binding_digest',execution_row.governed_tool_binding_digest,
          'provider_connection_contract_digest',execution_row.provider_connection_contract_digest,
          'provider_capability_digest',execution_row.provider_capability_digest,
          'provider_operation_id',execution_row.provider_operation_id,
          'provider_operation_descriptor_digest',execution_row.provider_operation_descriptor_digest,
          'policy_digest',execution_row.policy_digest));
      IF execution_row.execution_binding_hash IS DISTINCT FROM execution_binding_value
         OR payload->>'execution_binding_hash' IS DISTINCT FROM execution_binding_value
         OR payload->>'okg_claim_handoff_receipt_digest' IS DISTINCT FROM execution_row.okg_claim_handoff_receipt_digest
         OR payload->>'provider_idempotency_key_digest' IS DISTINCT FROM execution_row.provider_idempotency_key_digest THEN
        RAISE EXCEPTION 'restore checkpoint execution semantic hash is invalid' USING ERRCODE='40001';
      END IF;
      SELECT * INTO outcome_row FROM okg.external_action_outcomes
       WHERE deployment_name=bound_deployment AND execution_id=source_execution FOR SHARE;
      IF outcome_row.outcome_id IS NOT NULL THEN
        outcome_request_value:=okg.external_action_hash(
          'okg-external-action-outcome-request-v1',jsonb_build_object(
            'execution_id',outcome_row.execution_id,'effect_instance_key',outcome_row.effect_instance_key,
            'worker_or_reconciler_separation_subject',outcome_row.worker_or_reconciler_separation_subject,
            'outcome',outcome_row.outcome,'outcome_reason_code',outcome_row.outcome_reason_code,
            'provider_receipt_digest',outcome_row.provider_receipt_digest,
            'recovery_journal_digest',outcome_row.recovery_journal_digest,
            'owner_unavailability_attestation_digest',outcome_row.owner_unavailability_attestation_digest));
        IF outcome_row.request_hash IS DISTINCT FROM outcome_request_value THEN
          RAISE EXCEPTION 'restore checkpoint outcome semantic hash is invalid' USING ERRCODE='40001';
        END IF;
        IF payload->>'outcome' IS NOT NULL AND (
             payload->>'outcome' IS DISTINCT FROM outcome_row.outcome
             OR payload->>'outcome_reason_code' IS DISTINCT FROM outcome_row.outcome_reason_code
             OR payload->>'provider_receipt_digest' IS DISTINCT FROM outcome_row.provider_receipt_digest
             OR payload->>'journal_digest' IS DISTINCT FROM outcome_row.recovery_journal_digest) THEN
          RAISE EXCEPTION 'restore reconciliation is terminal: checkpoint conflicts with existing terminal outcome'
            USING ERRCODE='40001',
              DETAIL=jsonb_build_object('code','restore_reconciliation_conflict',
                'classification','database_restore_terminal_conflict',
                'effect_instance_key',entry.effect_instance_key,
                'retained_outcome',outcome_row.outcome,
                'signed_outcome',payload->>'outcome',
                'retained_outcome_reason_code',outcome_row.outcome_reason_code,
                'signed_outcome_reason_code',payload->>'outcome_reason_code')::text;
        END IF;
      END IF;
    END IF;
    SELECT COALESCE(max(e.sequence),0),
           (array_agg(e.event_hash ORDER BY e.sequence))[array_length(array_agg(e.event_hash ORDER BY e.sequence),1)]
      INTO previous_sequence,previous_hash FROM okg.external_action_events e
     WHERE e.deployment_name=bound_deployment AND e.effect_instance_key=entry.effect_instance_key;
    IF payload->>'pre_restore_effect_event_head_hash' IS DISTINCT FROM previous_hash THEN
      RAISE EXCEPTION 'restore checkpoint event head is stale' USING ERRCODE='40001';
    END IF;
    effective_value:=COALESCE(NULLIF(payload->>'effect_started_at','')::timestamptz,
      NULLIF(payload->>'outcome_recorded_at','')::timestamptz,
      NULLIF(payload->>'checkpoint_issued_at','')::timestamptz);
    IF effective_value IS NULL THEN
      RAISE EXCEPTION 'restore checkpoint has no signed effective time' USING ERRCODE='40001';
    END IF;
    before_review:=proposal_row.review_state; before_execution:=proposal_row.execution_state;
    after_review:=before_review; after_execution:=before_execution;
    lifecycle_value:=COALESCE(proposal_row.lifecycle_version,1);
    payload_version_value:=proposal_row.payload_version;
    event_outcome:=NULL; event_reason:=classification; projection_changed:=FALSE;
    IF classification='database_restore_no_effect_started' THEN
      IF proposal_row.proposal_id IS NOT NULL AND source_execution IS NULL
         AND before_review IN ('rejected','expired','cancelled') THEN
        classification:='database_restore_terminal_review_confirmed';
      ELSIF proposal_row.proposal_id IS NOT NULL THEN
        after_review:='cancelled'; after_execution:='unavailable'; projection_changed:=TRUE;
      END IF;
    ELSIF classification IN ('database_restore_effect_uncertain','database_restore_succeeded','database_restore_failed') THEN
      IF outcome_row.outcome_id IS NOT NULL THEN
        IF payload->>'outcome' IS NOT NULL AND outcome_row.outcome<>payload->>'outcome' THEN
          RAISE EXCEPTION 'restore reconciliation is terminal: checkpoint conflicts with existing terminal outcome'
            USING ERRCODE='40001',
              DETAIL=jsonb_build_object('code','restore_reconciliation_conflict',
                'classification','database_restore_terminal_conflict',
                'effect_instance_key',entry.effect_instance_key,
                'retained_outcome',outcome_row.outcome,
                'signed_outcome',payload->>'outcome')::text;
        END IF;
        classification:='database_restore_terminal_confirmed';
        event_outcome:=outcome_row.outcome;
      ELSIF proposal_row.proposal_id IS NOT NULL THEN
        after_review:='approved';
        after_execution:=CASE classification WHEN 'database_restore_succeeded' THEN 'succeeded' WHEN 'database_restore_failed' THEN 'failed' ELSE 'indeterminate' END;
        projection_changed:=TRUE;
        event_outcome:=CASE classification WHEN 'database_restore_succeeded' THEN 'succeeded' WHEN 'database_restore_failed' THEN 'failed' ELSE COALESCE(payload->>'outcome','indeterminate') END;
      ELSE
        event_outcome:=CASE classification WHEN 'database_restore_succeeded' THEN 'succeeded' WHEN 'database_restore_failed' THEN 'failed' ELSE COALESCE(payload->>'outcome','indeterminate') END;
      END IF;
    END IF;
    -- The database projection may refine a journal-derived class (for
    -- example, a surviving terminal review or an already recorded outcome).
    -- The event reason must name that final class, never the unrefined input.
    event_reason:=classification;
    IF classification='database_restore_terminal_confirmed' AND event_outcome IS NULL THEN
      event_outcome:=outcome_row.outcome;
    END IF;
    -- The recovery projection records the durable outcome selected by the
    -- classifier.  In particular, an uncertain ledger phase is an explicit
    -- indeterminate outcome, and a matching pre-existing outcome must not be
    -- lost merely because the signed checkpoint omitted its outcome fields.
    recovered_outcome_value:=COALESCE(outcome_row.outcome,event_outcome,NULLIF(payload->>'outcome',''));
    classification_counts:=classification_counts||jsonb_build_object(
      classification,COALESCE((classification_counts->>classification)::bigint,0)+1);
    IF projection_changed THEN lifecycle_value:=lifecycle_value+1; END IF;
    submission_hash:=COALESCE(proposal_row.submission_content_hash,NULLIF(payload->>'submission_content_hash',''));
    IF submission_hash IS NULL THEN
      RAISE EXCEPTION 'restore checkpoint has no submission content binding' USING ERRCODE='40001';
    END IF;
    recovery_projection:=jsonb_build_object(
      'deployment',bound_deployment,'old_epoch',attestation_row.old_epoch,'new_epoch',attestation_row.new_epoch,
      'effect_instance_key',entry.effect_instance_key,'submission_content_hash',submission_hash,
      'checkpoint_digest',entry.checkpoint_envelope_digest,'restore_attestation_digest',attestation_row.envelope_digest,
      'classification',classification,'source_proposal_id',source_proposal,'source_decision_id',source_decision,
      'source_execution_id',source_execution,'recovered_review_state',after_review,
      'recovered_execution_state',after_execution,'recovered_outcome',recovered_outcome_value,
      'effective_at',to_char(effective_value AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
      'recorded_at',to_char(completed_value AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
      'pre_restore_effect_event_head_hash',previous_hash);
    recovery_digest_value:=okg.external_action_hash('okg-external-action-restore-recovery-v1',recovery_projection);
    event_sequence:=previous_sequence+1;
    event_projection:=jsonb_build_object(
      'proposal_id',source_proposal,'execution_epoch',attestation_row.new_epoch,
      'effect_instance_key',entry.effect_instance_key,'sequence',event_sequence,
      'lifecycle_version',lifecycle_value,'event_type','restore_recovered',
      'actor_kind','authenticated_principal','actor_principal',actor_principal_value,
      'actor_separation_subject',session_row.operator_separation_subject,
      'authentication_audit_ref',session_row.authentication_audit_id,
      'nomos_decision_ref',session_row.nomos_decision_id,'deadline_kind',NULL::TEXT,
      'policy_digest',policy_digest_value,'before_review_state',before_review,
      'after_review_state',after_review,'before_execution_state',before_execution,
      'after_execution_state',after_execution,'payload_version',payload_version_value,
      'payload_hash',proposal_row.current_payload_hash,'decision_id',source_decision,
      'decision_binding_hash',decision_row.decision_binding_hash,'execution_id',source_execution,
      'execution_binding_hash',execution_row.execution_binding_hash,'transition_reason_code',NULL::TEXT,
      'outcome',event_outcome,'outcome_reason_code',event_reason,'outcome_request_hash',NULL::TEXT,
      'restore_recovery_digest',recovery_digest_value,
      'effective_at',to_char(effective_value AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
      'recorded_at',to_char(completed_value AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
      'previous_event_hash',previous_hash);
    event_hash_value:=okg.external_action_hash('okg-external-action-event-v1',event_projection);
    recovery_items:=recovery_items||jsonb_build_array(recovery_projection||jsonb_build_object(
      'recovery_digest',recovery_digest_value,'restored_effect_event_head_hash',event_hash_value));
    event_items:=event_items||jsonb_build_array(event_projection||jsonb_build_object('event_hash',event_hash_value,'projection_changed',projection_changed));
    recovery_digests:=recovery_digests||jsonb_build_array(to_jsonb(recovery_digest_value));
    new_head:=jsonb_build_object('effect_instance_key',entry.effect_instance_key,'sequence',event_sequence,'event_hash',event_hash_value);
    SELECT COALESCE(jsonb_agg(h ORDER BY convert_to(h->>'effect_instance_key','UTF8')),'[]'::jsonb)
      INTO predicted_heads FROM jsonb_array_elements(predicted_heads) h
     WHERE h->>'effect_instance_key'<>entry.effect_instance_key;
    predicted_heads:=predicted_heads||jsonb_build_array(new_head);
    SELECT COALESCE(jsonb_agg(h ORDER BY convert_to(h->>'effect_instance_key','UTF8')),'[]'::jsonb)
      INTO predicted_heads FROM jsonb_array_elements(predicted_heads) h;
  END LOOP;
  SELECT COALESCE(jsonb_agg(d ORDER BY convert_to(d #>> '{}','UTF8')),'[]'::jsonb)
    INTO recovery_digests FROM jsonb_array_elements(recovery_digests) d;
  recovery_set:=jsonb_build_object('sorted_restore_recovery_digests',recovery_digests,'count',jsonb_array_length(recovery_digests));
  recovery_set:=recovery_set||jsonb_build_object('digest',okg.external_action_hash('okg-external-action-restore-recovery-set-v1',recovery_set));
  post_root:=jsonb_build_object('sorted_event_head_entries',predicted_heads,'count',jsonb_array_length(predicted_heads));
  post_root:=post_root||jsonb_build_object('digest',okg.external_action_hash('okg-external-action-restore-event-root-v1',post_root));
  RETURN jsonb_build_object(
    'projection',jsonb_build_object('deployment',bound_deployment,'restore_id',session_row.restore_id,
      'restore_attestation_envelope_digest',attestation_row.envelope_digest,
      'pre_restore_event_root_digest',pre_root->>'digest','pre_restore_event_head_count',(pre_root->>'count')::bigint,
      'restored_event_root_digest',post_root->>'digest','restored_event_head_count',(post_root->>'count')::bigint,
      'recovery_set_digest',recovery_set->>'digest','recovery_count',(recovery_set->>'count')::bigint,
      'completed_at',to_char(completed_value AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"')),
    'recoveries',recovery_items,'events',event_items,
    'classification_summary',classification_counts);
END
$external_action_restore_build_plan$;

-- Prepare is a read/plan operation.  A terminal idempotent replay is checked
-- before the restore-only guard so an exact committed replay remains safe
-- after the deployment has returned to normal service.
CREATE OR REPLACE FUNCTION okg.external_action_restore_prepare(p_command JSONB)
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,okg
AS $external_action_restore_prepare_final$
DECLARE bound_deployment TEXT:=okg.external_action_session_deployment();
  session_row okg.external_action_restore_sessions%ROWTYPE;
  existing_final okg.external_action_restore_finalize_idempotency%ROWTYPE;
  plan JSONB; plan_command JSONB; operator_value TEXT; key_value TEXT; request_value TEXT;
  expected_request_hash TEXT;
BEGIN
  PERFORM okg.external_action_verifier_capability_status();
  PERFORM okg.external_action_require_command(p_command,
    ARRAY['deployment','restore_id','restore_attestation_envelope_digest','expected_inventory_digest',
      'expected_inventory_count','expected_checkpoint_set_digest','expected_checkpoint_count',
      'operator_separation_subject','idempotency_key_hash','request_hash','authentication_audit_id','nomos_decision_id'],
    ARRAY['deployment','restore_id','restore_attestation_envelope_digest','expected_inventory_digest',
      'expected_inventory_count','expected_checkpoint_set_digest','expected_checkpoint_count',
      'operator_separation_subject','idempotency_key_hash','request_hash']);
  IF p_command->>'deployment' IS DISTINCT FROM bound_deployment THEN
    RAISE EXCEPTION 'external action deployment is not bound to session_user' USING ERRCODE='28000';
  END IF;
  operator_value:=p_command->>'operator_separation_subject'; key_value:=p_command->>'idempotency_key_hash'; request_value:=p_command->>'request_hash';
  -- Resolve the immutable session and current authority before the terminal
  -- replay lookup.  A matching key cannot bypass a revoked audit/decision,
  -- while an exact replay remains independent of the current service mode.
  PERFORM pg_advisory_xact_lock(hashtextextended('okg.external_action.deployment:'||bound_deployment,0));
  SELECT * INTO session_row FROM okg.external_action_restore_sessions
   WHERE deployment_name=bound_deployment AND restore_id=p_command->>'restore_id' FOR SHARE;
  IF session_row.restore_session_row_id IS NULL OR session_row.operator_separation_subject IS DISTINCT FROM operator_value THEN
    RAISE EXCEPTION 'restore plan operator is not session-bound' USING ERRCODE='42501';
  END IF;
  IF session_row.authentication_audit_id IS DISTINCT FROM p_command->>'authentication_audit_id'
     OR session_row.nomos_decision_id IS DISTINCT FROM p_command->>'nomos_decision_id' THEN
    RAISE EXCEPTION 'restore plan authority is not session-bound' USING ERRCODE='42501';
  END IF;
  PERFORM okg.external_action_nomos_authority_snapshot(bound_deployment,p_command->>'authentication_audit_id',p_command->>'nomos_decision_id','external_action.restore_reconcile',p_command->>'request_hash');
  SELECT * INTO existing_final FROM okg.external_action_restore_finalize_idempotency
   WHERE deployment_name=bound_deployment AND restore_id=p_command->>'restore_id' FOR SHARE;
  IF existing_final.finalize_idempotency_row_id IS NOT NULL THEN
    IF existing_final.operator_separation_subject IS DISTINCT FROM operator_value
       OR existing_final.idempotency_key_hash IS DISTINCT FROM key_value
       OR existing_final.request_hash IS DISTINCT FROM request_value THEN
      RAISE EXCEPTION 'restore finalize replay conflicts with the first operator, key, or request' USING ERRCODE='40001';
    END IF;
    RETURN jsonb_build_object('replay',TRUE,'response_body_jcs',jsonb_build_object('__okg_bytes_b64',encode(existing_final.response_body_jcs,'base64')),'completion_receipt_envelope_digest',existing_final.completion_receipt_envelope_digest);
  END IF;
  expected_request_hash:=okg.external_action_hash(
    'okg-external-action-restore-finalize-request-v1',jsonb_build_object(
      'restore_attestation_envelope_digest',p_command->>'restore_attestation_envelope_digest',
      'expected_inventory_digest',p_command->>'expected_inventory_digest',
      'expected_inventory_count',(p_command->>'expected_inventory_count')::bigint,
      'expected_checkpoint_set_digest',p_command->>'expected_checkpoint_set_digest',
      'expected_checkpoint_count',(p_command->>'expected_checkpoint_count')::bigint));
  IF request_value IS DISTINCT FROM expected_request_hash THEN
    RAISE EXCEPTION 'restore finalize request hash is not canonical' USING ERRCODE='40001';
  END IF;
  SELECT * INTO session_row FROM okg.external_action_restore_sessions
   WHERE deployment_name=bound_deployment AND restore_id=p_command->>'restore_id' FOR UPDATE;
  plan_command:=p_command-ARRAY['operator_separation_subject','idempotency_key_hash','request_hash'];
  plan:=okg.external_action_restore_build_plan(plan_command);
  RETURN (plan->'projection') || jsonb_build_object('replay',FALSE);
END
$external_action_restore_prepare_final$;

-- Finalize consumes only the exact projection returned by prepare.  All
-- recovery rows and hash-chain events are inserted before the persisted fence
-- can be switched back to normal; any failure rolls the transaction back.
CREATE OR REPLACE FUNCTION okg.external_action_restore_finalize(p_command JSONB)
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,okg
AS $external_action_restore_finalize_final$
DECLARE bound_deployment TEXT:=okg.external_action_session_deployment();
  session_row okg.external_action_restore_sessions%ROWTYPE;
  attestation_row okg.external_action_restore_attestations%ROWTYPE;
  existing_final okg.external_action_restore_finalize_idempotency%ROWTYPE;
  completion_row okg.external_action_restore_completion_receipts%ROWTYPE;
  fence_row okg.external_action_restore_fences%ROWTYPE;
  plan JSONB; projection JSONB; plan_projection JSONB; unsigned JSONB; coverage_command JSONB;
  recovery JSONB; event JSONB; event_projection JSONB; response JSONB;
  checkpoint_payload_for_recovery JSONB; outcome_to_record TEXT; outcome_reason_value TEXT; outcome_request_hash_value TEXT;
  operator_value TEXT; key_value TEXT; request_value TEXT; completed_value TIMESTAMPTZ;
  expected_request_hash TEXT;
BEGIN
  PERFORM okg.external_action_verifier_capability_status();
  PERFORM okg.external_action_require_command(p_command,
    ARRAY['deployment','restore_id','restore_attestation_envelope_digest','expected_inventory_digest',
      'expected_inventory_count','expected_checkpoint_set_digest','expected_checkpoint_count',
      'operator_separation_subject','idempotency_key_hash','request_hash','completion_receipt_envelope_digest',
      'completion_receipt_protected_b64','completion_receipt_payload_b64','completion_receipt_signature_b64',
      'completion_receipt_projection','completion_receipt_projection_digest','authentication_audit_id','nomos_decision_id'],
    ARRAY['deployment','restore_id','restore_attestation_envelope_digest','expected_inventory_digest',
      'expected_inventory_count','expected_checkpoint_set_digest','expected_checkpoint_count',
      'operator_separation_subject','idempotency_key_hash','request_hash','completion_receipt_envelope_digest',
      'completion_receipt_protected_b64','completion_receipt_payload_b64','completion_receipt_signature_b64',
      'completion_receipt_projection','completion_receipt_projection_digest','authentication_audit_id','nomos_decision_id']);
  IF p_command->>'deployment' IS DISTINCT FROM bound_deployment THEN
    RAISE EXCEPTION 'external action deployment is not bound to session_user' USING ERRCODE='28000';
  END IF;
  operator_value:=p_command->>'operator_separation_subject'; key_value:=p_command->>'idempotency_key_hash'; request_value:=p_command->>'request_hash';
  -- Resolve the immutable session and current authority before the terminal
  -- replay lookup.  A matching key cannot bypass a revoked audit/decision,
  -- while an exact replay remains independent of the current service mode.
  PERFORM pg_advisory_xact_lock(hashtextextended('okg.external_action.deployment:'||bound_deployment,0));
  SELECT * INTO session_row FROM okg.external_action_restore_sessions
   WHERE deployment_name=bound_deployment AND restore_id=p_command->>'restore_id' FOR SHARE;
  IF session_row.restore_session_row_id IS NULL OR session_row.operator_separation_subject IS DISTINCT FROM operator_value THEN
    RAISE EXCEPTION 'restore finalize operator is not session-bound' USING ERRCODE='42501';
  END IF;
  IF session_row.authentication_audit_id IS DISTINCT FROM p_command->>'authentication_audit_id'
     OR session_row.nomos_decision_id IS DISTINCT FROM p_command->>'nomos_decision_id' THEN
    RAISE EXCEPTION 'restore finalize authority is not session-bound' USING ERRCODE='42501';
  END IF;
  PERFORM okg.external_action_nomos_authority_snapshot(bound_deployment,p_command->>'authentication_audit_id',p_command->>'nomos_decision_id','external_action.restore_reconcile',p_command->>'request_hash');
  -- Resolve the terminal replay before any restore-only or expiry checks.
  SELECT * INTO existing_final FROM okg.external_action_restore_finalize_idempotency
   WHERE deployment_name=bound_deployment AND restore_id=p_command->>'restore_id' FOR SHARE;
  IF existing_final.finalize_idempotency_row_id IS NOT NULL THEN
    IF existing_final.operator_separation_subject IS DISTINCT FROM operator_value
       OR existing_final.idempotency_key_hash IS DISTINCT FROM key_value
       OR existing_final.request_hash IS DISTINCT FROM request_value THEN
      RAISE EXCEPTION 'restore finalize replay conflicts with the first operator, key, or request' USING ERRCODE='40001';
    END IF;
    RETURN convert_from(existing_final.response_body_jcs,'UTF8')::jsonb;
  END IF;
  expected_request_hash:=okg.external_action_hash(
    'okg-external-action-restore-finalize-request-v1',jsonb_build_object(
      'restore_attestation_envelope_digest',p_command->>'restore_attestation_envelope_digest',
      'expected_inventory_digest',p_command->>'expected_inventory_digest',
      'expected_inventory_count',(p_command->>'expected_inventory_count')::bigint,
      'expected_checkpoint_set_digest',p_command->>'expected_checkpoint_set_digest',
      'expected_checkpoint_count',(p_command->>'expected_checkpoint_count')::bigint));
  IF request_value IS DISTINCT FROM expected_request_hash THEN
    RAISE EXCEPTION 'restore finalize request hash is not canonical' USING ERRCODE='40001';
  END IF;
  SELECT * INTO session_row FROM okg.external_action_restore_sessions
   WHERE deployment_name=bound_deployment AND restore_id=p_command->>'restore_id' FOR UPDATE;
  SELECT * INTO completion_row FROM okg.external_action_restore_completion_receipts
   WHERE deployment_name=bound_deployment AND restore_id=session_row.restore_id FOR SHARE;
  IF completion_row.completion_receipt_row_id IS NOT NULL THEN
    RAISE EXCEPTION 'restore completion exists without its terminal idempotency record' USING ERRCODE='40001';
  END IF;
  SELECT * INTO attestation_row FROM okg.external_action_restore_attestations
   WHERE deployment_name=bound_deployment
     AND envelope_digest=session_row.restore_attestation_envelope_digest
   FOR SHARE;
  IF attestation_row.attestation_row_id IS NULL THEN
    RAISE EXCEPTION 'restore attestation projection is unavailable' USING ERRCODE='40001';
  END IF;
  projection:=p_command->'completion_receipt_projection';
  IF jsonb_typeof(projection) IS DISTINCT FROM 'object' THEN
    RAISE EXCEPTION 'completion receipt projection is not an object' USING ERRCODE='22023';
  END IF;
  completed_value:=NULLIF(projection->>'completed_at','')::timestamptz;
  IF completed_value IS NULL THEN
    RAISE EXCEPTION 'completion receipt has no completed_at' USING ERRCODE='22023';
  END IF;
  coverage_command:=jsonb_build_object('deployment',bound_deployment,'restore_id',session_row.restore_id,
    'restore_attestation_envelope_digest',p_command->>'restore_attestation_envelope_digest',
    'expected_inventory_digest',p_command->>'expected_inventory_digest','expected_inventory_count',p_command->>'expected_inventory_count',
    'expected_checkpoint_set_digest',p_command->>'expected_checkpoint_set_digest','expected_checkpoint_count',p_command->>'expected_checkpoint_count',
    'completed_at',to_char(completed_value AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'));
  plan:=okg.external_action_restore_build_plan(coverage_command);
  plan_projection:=plan->'projection';
  IF projection IS DISTINCT FROM plan_projection THEN
    RAISE EXCEPTION 'completion receipt projection is stale or does not match the immutable plan' USING ERRCODE='40001';
  END IF;
  IF okg.external_action_hash('okg-external-action-restore-completion-receipt-v1',projection) IS DISTINCT FROM p_command->>'completion_receipt_projection_digest' THEN
    RAISE EXCEPTION 'completion receipt projection digest is invalid' USING ERRCODE='22023';
  END IF;
  unsigned:=jsonb_build_object('protected_b64',p_command->>'completion_receipt_protected_b64','payload_b64',p_command->>'completion_receipt_payload_b64','signature_b64',p_command->>'completion_receipt_signature_b64');
  IF p_command->>'completion_receipt_envelope_digest' IS DISTINCT FROM okg.external_action_hash('okg-external-action-restore-completion-receipt-envelope-v1',unsigned) THEN
    RAISE EXCEPTION 'completion receipt envelope digest is invalid' USING ERRCODE='22023';
  END IF;
  IF p_command->>'completion_receipt_protected_b64' !~ '^[A-Za-z0-9_-]+$' OR p_command->>'completion_receipt_payload_b64' !~ '^[A-Za-z0-9_-]+$' OR p_command->>'completion_receipt_signature_b64' !~ '^[A-Za-z0-9_-]+$' THEN
    RAISE EXCEPTION 'completion receipt envelope encoding is invalid' USING ERRCODE='22023';
  END IF;
  FOR recovery IN SELECT value FROM jsonb_array_elements(plan->'recoveries') LOOP
    -- A recovered execution still needs the durable outcome row consumed by
    -- the normal projection.  The restore operator records only a database
    -- observation; this never performs provider I/O.
    IF recovery->>'source_execution_id' IS NOT NULL
       AND recovery->>'classification' IN ('database_restore_effect_uncertain','database_restore_succeeded','database_restore_failed')
       AND NOT EXISTS (
         SELECT 1 FROM okg.external_action_outcomes o
          WHERE o.deployment_name=bound_deployment AND o.execution_id=recovery->>'source_execution_id') THEN
      SELECT e.checkpoint_payload INTO checkpoint_payload_for_recovery
        FROM okg.external_action_restore_checkpoint_entries e
       WHERE e.deployment_name=bound_deployment AND e.restore_id=session_row.restore_id
         AND e.effect_instance_key=recovery->>'effect_instance_key';
      outcome_to_record:=CASE recovery->>'classification'
        WHEN 'database_restore_succeeded' THEN 'succeeded'
        WHEN 'database_restore_failed' THEN 'failed'
        ELSE COALESCE(NULLIF(checkpoint_payload_for_recovery->>'outcome',''),'indeterminate') END;
      -- A restore observation must never invent a reason outside the retained
      -- action contract. Prefer the phase-specific contract code when the
      -- contract carries it (provider_receipt_missing for an uncertain phase,
      -- provider_receipt_observed for a receipt, and
      -- provider_explicit_failure for an explicit failure); fall back to the
      -- lexicographically first valid code for deterministic legacy contracts.
      outcome_reason_value:=NULLIF(checkpoint_payload_for_recovery->>'outcome_reason_code','');
      IF outcome_reason_value IS NULL THEN
        SELECT COALESCE(
          min(r.reason_code) FILTER (WHERE r.reason_code = CASE outcome_to_record
            WHEN 'indeterminate' THEN 'provider_receipt_missing'
            WHEN 'succeeded' THEN 'provider_receipt_observed'
            WHEN 'failed' THEN 'provider_explicit_failure'
          END),
          min(r.reason_code))
          INTO outcome_reason_value
          FROM okg.external_action_proposals p
          CROSS JOIN LATERAL jsonb_each(p.action_contract_record->'outcome_reason_rules') AS r(reason_code,rule)
         WHERE p.deployment_name=bound_deployment
           AND p.proposal_id=recovery->>'source_proposal_id'
           AND (r.rule->'allowed_outcomes' ? outcome_to_record)
           AND r.rule->>'provider_receipt' = CASE
             WHEN checkpoint_payload_for_recovery->>'provider_receipt_digest' IS NULL
               THEN 'forbidden' ELSE 'required' END;
        IF outcome_reason_value IS NULL THEN
          RAISE EXCEPTION 'restore outcome has no retained action-contract reason'
            USING ERRCODE='40001',
              DETAIL=jsonb_build_object('code','restore_contract_reason_unavailable',
                'classification',recovery->>'classification',
                'effect_instance_key',recovery->>'effect_instance_key',
                'outcome',outcome_to_record)::text;
        END IF;
      END IF;
      IF outcome_reason_value IS NULL THEN
        RAISE EXCEPTION 'restore outcome has no retained action-contract reason'
          USING ERRCODE='40001',
            DETAIL=jsonb_build_object('classification',recovery->>'classification',
              'effect_instance_key',recovery->>'effect_instance_key')::text;
      END IF;
      outcome_request_hash_value:=okg.external_action_hash(
        'okg-external-action-outcome-request-v1',jsonb_build_object(
          'execution_id',recovery->>'source_execution_id',
          'effect_instance_key',recovery->>'effect_instance_key',
          'worker_or_reconciler_separation_subject',session_row.operator_separation_subject,
          'outcome',outcome_to_record,
          'outcome_reason_code',outcome_reason_value,
          'provider_receipt_digest',checkpoint_payload_for_recovery->>'provider_receipt_digest',
          'recovery_journal_digest',checkpoint_payload_for_recovery->>'journal_digest',
          'owner_unavailability_attestation_digest',NULL));
      INSERT INTO okg.external_action_outcomes
        (deployment_name,execution_id,effect_instance_key,worker_or_reconciler_separation_subject,
         outcome,outcome_reason_code,provider_receipt_digest,recovery_journal_digest,
         owner_unavailability_attestation_digest,request_hash,authentication_audit_id,
         nomos_decision_id,recorded_at,store_record)
      VALUES (bound_deployment,recovery->>'source_execution_id',recovery->>'effect_instance_key',
        session_row.operator_separation_subject,outcome_to_record,
        outcome_reason_value,
        checkpoint_payload_for_recovery->>'provider_receipt_digest',
        COALESCE(NULLIF(checkpoint_payload_for_recovery->>'journal_digest',''),recovery->>'recovery_digest'),
        NULL,outcome_request_hash_value,session_row.authentication_audit_id,
        session_row.nomos_decision_id,completed_value,recovery);
    END IF;
    INSERT INTO okg.external_action_restore_recoveries
      (deployment_name,old_epoch,new_epoch,effect_instance_key,submission_content_hash,checkpoint_digest,restore_attestation_digest,classification,source_proposal_id,source_decision_id,source_execution_id,recovered_review_state,recovered_execution_state,recovered_outcome,effective_at,recorded_at,pre_restore_effect_event_head_hash,restored_effect_event_head_hash,signed_worker_checkpoint_digest,recovery_digest,store_record)
    VALUES (bound_deployment,(SELECT old_epoch FROM okg.external_action_restore_attestations WHERE deployment_name=bound_deployment AND envelope_digest=session_row.restore_attestation_envelope_digest),
      (SELECT new_epoch FROM okg.external_action_restore_attestations WHERE deployment_name=bound_deployment AND envelope_digest=session_row.restore_attestation_envelope_digest),
      recovery->>'effect_instance_key',recovery->>'submission_content_hash',recovery->>'checkpoint_digest',session_row.restore_attestation_envelope_digest,recovery->>'classification',NULLIF(recovery->>'source_proposal_id',''),NULLIF(recovery->>'source_decision_id',''),NULLIF(recovery->>'source_execution_id',''),NULLIF(recovery->>'recovered_review_state',''),NULLIF(recovery->>'recovered_execution_state',''),NULLIF(recovery->>'recovered_outcome',''),(recovery->>'effective_at')::timestamptz,(recovery->>'recorded_at')::timestamptz,NULLIF(recovery->>'pre_restore_effect_event_head_hash',''),recovery->>'restored_effect_event_head_hash',recovery->>'checkpoint_digest',recovery->>'recovery_digest',recovery);
  END LOOP;
  FOR event IN SELECT value FROM jsonb_array_elements(plan->'events') LOOP
    event_projection:=event-'event_hash'-'projection_changed';
    INSERT INTO okg.external_action_events
      (deployment_name,proposal_id,execution_epoch,effect_instance_key,sequence,lifecycle_version,event_type,actor_kind,actor_principal,actor_separation_subject,authentication_audit_ref,nomos_decision_ref,deadline_kind,policy_digest,before_review_state,after_review_state,before_execution_state,after_execution_state,payload_version,payload_hash,decision_id,decision_binding_hash,execution_id,execution_binding_hash,transition_reason_code,outcome,outcome_reason_code,outcome_request_hash,restore_recovery_digest,effective_at,recorded_at,previous_event_hash,event_hash,store_record)
    VALUES (bound_deployment,NULLIF(event_projection->>'proposal_id',''),(event_projection->>'execution_epoch')::bigint,event_projection->>'effect_instance_key',(event_projection->>'sequence')::bigint,(event_projection->>'lifecycle_version')::bigint,event_projection->>'event_type',event_projection->>'actor_kind',event_projection->>'actor_principal',event_projection->>'actor_separation_subject',event_projection->>'authentication_audit_ref',event_projection->>'nomos_decision_ref',NULLIF(event_projection->>'deadline_kind',''),event_projection->>'policy_digest',NULLIF(event_projection->>'before_review_state',''),NULLIF(event_projection->>'after_review_state',''),NULLIF(event_projection->>'before_execution_state',''),NULLIF(event_projection->>'after_execution_state',''),NULLIF(event_projection->>'payload_version','')::bigint,event_projection->>'payload_hash',NULLIF(event_projection->>'decision_id',''),event_projection->>'decision_binding_hash',NULLIF(event_projection->>'execution_id',''),event_projection->>'execution_binding_hash',NULLIF(event_projection->>'transition_reason_code',''),event_projection->>'outcome',event_projection->>'outcome_reason_code',event_projection->>'outcome_request_hash',event_projection->>'restore_recovery_digest',(event_projection->>'effective_at')::timestamptz,(event_projection->>'recorded_at')::timestamptz,event_projection->>'previous_event_hash',event->>'event_hash',event_projection);
    IF event->>'projection_changed'='true' AND event->>'proposal_id' IS NOT NULL THEN
      UPDATE okg.external_action_proposals SET review_state=event->>'after_review_state',execution_state=event->>'after_execution_state',lifecycle_version=(event->>'lifecycle_version')::bigint,updated_at=completed_value
       WHERE deployment_name=bound_deployment AND proposal_id=event->>'proposal_id';
    END IF;
  END LOOP;
  response:=jsonb_build_object('replay',FALSE,'deployment',bound_deployment,'restore_id',session_row.restore_id,'state','complete','quarantine_state','cleared','service_mode','normal','recovery_count',(plan_projection->>'recovery_count')::bigint,'recovery_set_digest',plan_projection->>'recovery_set_digest','classification_summary',plan->'classification_summary','classifications',plan->'classification_summary','restore_completion_receipt',jsonb_build_object('envelope_digest',p_command->>'completion_receipt_envelope_digest'));
  INSERT INTO okg.external_action_restore_completion_receipts
    (deployment_name,restore_id,restore_attestation_envelope_digest,completion_receipt_envelope_digest,completed_at,authentication_audit_id,nomos_decision_id,store_record,protected_b64,payload_b64,signature_b64,completion_receipt_projection)
  VALUES (bound_deployment,session_row.restore_id,session_row.restore_attestation_envelope_digest,p_command->>'completion_receipt_envelope_digest',completed_value,p_command->>'authentication_audit_id',p_command->>'nomos_decision_id',response,p_command->>'completion_receipt_protected_b64',p_command->>'completion_receipt_payload_b64',p_command->>'completion_receipt_signature_b64',projection);
  INSERT INTO okg.external_action_restore_finalize_idempotency
    (deployment_name,restore_id,operator_separation_subject,idempotency_key_hash,request_hash,response_body_jcs,completion_receipt_envelope_digest)
  VALUES (bound_deployment,session_row.restore_id,operator_value,key_value,request_value,okg.external_action_json_bytes(response),p_command->>'completion_receipt_envelope_digest');
  SELECT * INTO fence_row FROM okg.external_action_restore_fences WHERE deployment_name=bound_deployment FOR UPDATE;
  IF fence_row.service_mode<>'restore_only' OR fence_row.quarantine_state<>'blocked' OR fence_row.highest_accepted_epoch>attestation_row.new_epoch THEN
    RAISE EXCEPTION 'restore fence changed while finalizing' USING ERRCODE='40001';
  END IF;
  UPDATE okg.external_action_restore_fences SET highest_accepted_epoch=attestation_row.new_epoch,lease_digest=attestation_row.new_epoch_lease_digest,quarantine_state='clear',service_mode='normal',restore_attestation_digest=attestation_row.envelope_digest,accepted_at=completed_value,mode_changed_at=completed_value WHERE deployment_name=bound_deployment;
  RETURN response;
END
$external_action_restore_finalize_final$;

-- Final ACL convergence for functions defined after the historical grants.
DO $external_action_restore_final_acl$
BEGIN
  REVOKE ALL ON FUNCTION okg.external_action_reject_mutation() FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_monotonic_nomos_updated_at() FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_nomos_authority_lock() FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_restore_create(JSONB) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_restore_batch(JSONB) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_restore_prepare(JSONB) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_restore_finalize(JSONB) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_restore_classification(JSONB,TEXT) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_restore_build_plan(JSONB) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_restore_inventory_digest(JSONB) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_restore_inventory(TEXT) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_restore_checkpoint_set_digest(TEXT) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_restore_event_root(TEXT) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_restore_assert_coverage(JSONB) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_restore_lifecycle_gate() FROM PUBLIC;
  IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname='okg_external_action_api') THEN
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_create(JSONB) TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_batch(JSONB) TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_prepare(JSONB) TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_finalize(JSONB) TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_inventory(TEXT) TO okg_external_action_api;
  END IF;
  IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname='okg_external_action_function_owner') THEN
    ALTER FUNCTION okg.external_action_restore_create(JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_restore_batch(JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_restore_prepare(JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_restore_finalize(JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_restore_classification(JSONB,TEXT) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_restore_build_plan(JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_restore_inventory_digest(JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_restore_inventory(TEXT) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_restore_checkpoint_set_digest(TEXT) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_restore_event_root(TEXT) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_restore_assert_coverage(JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_restore_lifecycle_gate() OWNER TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_create(JSONB) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_batch(JSONB) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_prepare(JSONB) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_finalize(JSONB) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_classification(JSONB,TEXT) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_build_plan(JSONB) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_inventory_digest(JSONB) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_inventory(TEXT) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_checkpoint_set_digest(TEXT) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_event_root(TEXT) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_assert_coverage(JSONB) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_lifecycle_gate() TO okg_external_action_function_owner;
  END IF;
  IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname='okg_external_action_api') THEN
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_create(JSONB) TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_batch(JSONB) TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_prepare(JSONB) TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_finalize(JSONB) TO okg_external_action_api;
  END IF;
END
$external_action_restore_final_acl$;

-- Reassert API privileges after the final owner transfer and before the last
-- helper replacement.  These are the only restore functions exposed to the
-- API role; all planning/validation helpers remain function-owner only.
GRANT EXECUTE ON FUNCTION okg.external_action_restore_create(JSONB) TO okg_external_action_api;
GRANT EXECUTE ON FUNCTION okg.external_action_restore_batch(JSONB) TO okg_external_action_api;
GRANT EXECUTE ON FUNCTION okg.external_action_restore_prepare(JSONB) TO okg_external_action_api;
GRANT EXECUTE ON FUNCTION okg.external_action_restore_finalize(JSONB) TO okg_external_action_api;
GRANT EXECUTE ON FUNCTION okg.external_action_restore_inventory(TEXT) TO okg_external_action_api;

CREATE OR REPLACE FUNCTION okg.external_action_restore_assert_coverage(p_command JSONB)
RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $external_action_restore_assert_coverage$
DECLARE bound_deployment TEXT:=okg.external_action_session_deployment();
  session_row okg.external_action_restore_sessions%ROWTYPE;
  attestation_row okg.external_action_restore_attestations%ROWTYPE;
  fence_row okg.external_action_restore_fences%ROWTYPE;
  keys JSONB; inventory JSONB; db_inventory JSONB; derived_inventory JSONB; checkpoint_set JSONB; count_value BIGINT;
  inventory_count BIGINT; derived_inventory_count BIGINT; derived_inventory_digest TEXT;
  expected_inventory_digest TEXT; expected_checkpoint_digest TEXT; pre_root JSONB;
  positive_count BIGINT; positive_distinct_count BIGINT; positive_min BIGINT; positive_max BIGINT; positive_previous TEXT; last_positive_key TEXT;
BEGIN
  PERFORM okg.external_action_require_command(
    p_command,
    ARRAY['deployment','restore_id','restore_attestation_envelope_digest',
      'expected_inventory_digest','expected_inventory_count',
      'expected_checkpoint_set_digest','expected_checkpoint_count'],
    ARRAY['deployment','restore_id','restore_attestation_envelope_digest',
      'expected_inventory_digest','expected_inventory_count',
      'expected_checkpoint_set_digest','expected_checkpoint_count']
  );
  IF p_command->>'deployment' IS DISTINCT FROM bound_deployment THEN
    RAISE EXCEPTION 'external action deployment is not bound to session_user' USING ERRCODE='28000';
  END IF;
  SELECT * INTO session_row FROM okg.external_action_restore_sessions
   WHERE deployment_name=bound_deployment AND restore_id=p_command->>'restore_id' FOR UPDATE;
  IF session_row.restore_session_row_id IS NULL THEN
    RAISE EXCEPTION 'restore session is not finalizable' USING ERRCODE='40001';
  END IF;
  SELECT * INTO fence_row FROM okg.external_action_restore_fences
   WHERE deployment_name=bound_deployment FOR UPDATE;
  IF fence_row.deployment_name IS NULL OR fence_row.service_mode<>'restore_only'
     OR fence_row.quarantine_state<>'blocked' THEN
    RAISE EXCEPTION 'restore fence is not blocked in restore_only mode' USING ERRCODE='40001';
  END IF;
  IF session_row.restore_attestation_envelope_digest IS DISTINCT FROM
     p_command->>'restore_attestation_envelope_digest' THEN
    RAISE EXCEPTION 'restore attestation does not match session' USING ERRCODE='40001';
  END IF;
  SELECT * INTO attestation_row FROM okg.external_action_restore_attestations
   WHERE deployment_name=bound_deployment AND envelope_digest=session_row.restore_attestation_envelope_digest
   FOR SHARE;
  IF attestation_row.attestation_row_id IS NULL THEN
    RAISE EXCEPTION 'restore attestation projection is unavailable' USING ERRCODE='40001';
  END IF;
  pre_root:=okg.external_action_restore_event_root(bound_deployment);
  IF pre_root->>'digest' IS DISTINCT FROM attestation_row.pre_restore_event_root_digest
     OR (pre_root->>'count')::bigint IS DISTINCT FROM attestation_row.pre_restore_event_head_count THEN
    RAISE EXCEPTION 'restore pre-event root or count is stale' USING ERRCODE='40001';
  END IF;
  IF (p_command->>'expected_inventory_count')::bigint<>attestation_row.effect_inventory_count
     OR p_command->>'expected_inventory_digest'<>attestation_row.effect_inventory_digest
     OR (p_command->>'expected_checkpoint_count')::bigint<>attestation_row.worker_checkpoint_count
     OR p_command->>'expected_checkpoint_set_digest'<>attestation_row.worker_checkpoint_set_digest THEN
    RAISE EXCEPTION 'restore attestation root or count disagrees with finalize request' USING ERRCODE='40001';
  END IF;
  inventory:=COALESCE(session_row.effect_inventory,'[]'::jsonb);
  inventory_count:=jsonb_array_length(inventory);
  expected_inventory_digest:=okg.external_action_restore_inventory_digest(inventory);
  IF inventory_count<>session_row.effect_inventory_count
     OR session_row.effect_inventory_digest IS DISTINCT FROM expected_inventory_digest THEN
    RAISE EXCEPTION 'restore database inventory projection is not canonical' USING ERRCODE='40001';
  END IF;
  SELECT count(*)::bigint,
         COALESCE(jsonb_agg(to_jsonb(effect_instance_key)
           ORDER BY convert_to(effect_instance_key,'UTF8')),'[]'::jsonb)
    INTO count_value,keys
    FROM okg.external_action_restore_checkpoint_entries
   WHERE deployment_name=bound_deployment AND restore_id=session_row.restore_id;
  IF count_value<>attestation_row.worker_checkpoint_count THEN
    RAISE EXCEPTION 'restore checkpoint coverage is incomplete or has an extra effect key'
      USING ERRCODE='40001',
        DETAIL=jsonb_build_object('code','incomplete_restore_coverage',
          'quarantine_state','blocked',
          'expected_checkpoint_count',attestation_row.worker_checkpoint_count,
          'accepted_checkpoint_count',count_value)::text;
  END IF;
  checkpoint_set:=okg.external_action_restore_checkpoint_set_digest(session_row.restore_id);
  expected_checkpoint_digest:=checkpoint_set->>'digest';
  IF checkpoint_set->>'count'<>(p_command->>'expected_checkpoint_count')
     OR expected_checkpoint_digest<>p_command->>'expected_checkpoint_set_digest' THEN
    RAISE EXCEPTION 'restore checkpoint set digest or count is not exact' USING ERRCODE='40001';
  END IF;
  IF EXISTS (
    SELECT 1 FROM okg.external_action_restore_checkpoint_entries e
     WHERE e.deployment_name=bound_deployment AND e.restore_id=session_row.restore_id
       AND (jsonb_typeof(e.checkpoint_payload) IS DISTINCT FROM 'object'
            OR e.checkpoint_payload->>'proof_kind' NOT IN ('positive','nonmembership'))
  ) THEN
    RAISE EXCEPTION 'restore checkpoint lacks a verified recovery projection' USING ERRCODE='40001';
  END IF;
  IF EXISTS (
    SELECT 1 FROM okg.external_action_restore_checkpoint_entries e
     WHERE e.deployment_name=bound_deployment AND e.restore_id=session_row.restore_id
       AND (
         e.checkpoint_payload->>'deployment' IS DISTINCT FROM bound_deployment
         OR e.checkpoint_payload->>'effect_instance_key' IS DISTINCT FROM e.effect_instance_key
         OR e.checkpoint_payload->>'execution_epoch' IS DISTINCT FROM attestation_row.old_epoch::text
         OR e.checkpoint_payload->>'ledger_fence_envelope_digest' IS DISTINCT FROM attestation_row.ledger_fence_envelope_digest
         OR e.checkpoint_payload->>'fenced_ledger_root_digest' IS DISTINCT FROM attestation_row.fenced_ledger_root_digest
         OR e.checkpoint_payload->>'fenced_ledger_entry_count' IS DISTINCT FROM attestation_row.fenced_ledger_entry_count::text
         OR e.checkpoint_payload->>'checkpoint_issued_at' IS NULL
         OR e.checkpoint_payload->>'submission_content_hash' IS NULL
         OR (e.checkpoint_payload->>'proof_kind'='positive'
             AND e.checkpoint_payload->>'positive_proof_digest' IS DISTINCT FROM e.proof_digest)
         OR (e.checkpoint_payload->>'proof_kind'='nonmembership'
             AND e.checkpoint_payload->>'nonmembership_proof_digest' IS DISTINCT FROM e.proof_digest)
       )
  ) THEN
    RAISE EXCEPTION 'restore checkpoint is not bound to the attested fence projection' USING ERRCODE='40001';
  END IF;
  IF EXISTS (
    SELECT 1 FROM okg.external_action_restore_checkpoint_entries e
     WHERE e.deployment_name=bound_deployment AND e.restore_id=session_row.restore_id
       AND e.checkpoint_payload->>'proof_kind'='positive'
       AND (e.leaf_index IS NULL OR e.leaf_index < 0)
  ) THEN
    RAISE EXCEPTION 'positive checkpoint index is not canonical decimal' USING ERRCODE='40001';
  END IF;
  SELECT count(*) FILTER (WHERE e.checkpoint_payload->>'proof_kind'='positive'),
         count(DISTINCT e.leaf_index) FILTER (WHERE e.checkpoint_payload->>'proof_kind'='positive'),
         min(e.leaf_index) FILTER (WHERE e.checkpoint_payload->>'proof_kind'='positive'),
         max(e.leaf_index) FILTER (WHERE e.checkpoint_payload->>'proof_kind'='positive')
    INTO positive_count,positive_distinct_count,positive_min,positive_max
    FROM okg.external_action_restore_checkpoint_entries e
   WHERE e.deployment_name=bound_deployment AND e.restore_id=session_row.restore_id;
  IF positive_count<>attestation_row.fenced_ledger_entry_count
     OR positive_distinct_count<>positive_count
     OR (positive_count>0 AND (positive_min<>0 OR positive_max<>positive_count-1)) THEN
    RAISE EXCEPTION 'positive checkpoint indexes do not cover the fenced ledger exactly' USING ERRCODE='40001';
  END IF;
  last_positive_key:=NULL;
  FOR positive_previous IN SELECT e.effect_instance_key FROM okg.external_action_restore_checkpoint_entries e
    WHERE e.deployment_name=bound_deployment AND e.restore_id=session_row.restore_id
      AND e.checkpoint_payload->>'proof_kind'='positive'
    ORDER BY e.leaf_index LOOP
    IF positive_previous IS NULL OR btrim(positive_previous)='' THEN
      RAISE EXCEPTION 'positive checkpoint has no effect key' USING ERRCODE='40001';
    END IF;
    IF last_positive_key IS NOT NULL AND convert_to(positive_previous,'UTF8')<=convert_to(last_positive_key,'UTF8') THEN
      RAISE EXCEPTION 'positive checkpoint ledger keys are not strictly sorted' USING ERRCODE='40001';
    END IF;
    last_positive_key:=positive_previous;
  END LOOP;
  SELECT COALESCE(jsonb_agg(to_jsonb(effect_instance_key)
      ORDER BY convert_to(effect_instance_key,'UTF8')),'[]'::jsonb)
    INTO db_inventory
    FROM (
      SELECT effect_instance_key FROM okg.external_action_proposals
       WHERE deployment_name=bound_deployment AND execution_epoch=attestation_row.old_epoch
      UNION
      SELECT d.effect_instance_key FROM okg.external_action_decisions d
       JOIN okg.external_action_proposals p ON p.deployment_name=d.deployment_name AND p.proposal_id=d.proposal_id
       WHERE d.deployment_name=bound_deployment AND p.execution_epoch=attestation_row.old_epoch
      UNION
      SELECT effect_instance_key FROM okg.external_action_executions
       WHERE deployment_name=bound_deployment AND execution_epoch=attestation_row.old_epoch
      UNION
      SELECT o.effect_instance_key FROM okg.external_action_outcomes o
       JOIN okg.external_action_executions e ON e.deployment_name=o.deployment_name AND e.execution_id=o.execution_id
       WHERE o.deployment_name=bound_deployment AND e.execution_epoch=attestation_row.old_epoch
      UNION
      SELECT effect_instance_key FROM okg.external_action_events
       WHERE deployment_name=bound_deployment AND execution_epoch=attestation_row.old_epoch
    ) database_inventory;
  IF db_inventory IS DISTINCT FROM inventory THEN
    RAISE EXCEPTION 'restore database inventory projection is not the retained old-epoch set' USING ERRCODE='40001';
  END IF;
  SELECT COALESCE(jsonb_agg(to_jsonb(effect_instance_key)
      ORDER BY convert_to(effect_instance_key,'UTF8')),'[]'::jsonb)
    INTO derived_inventory
    FROM (
      SELECT value #>> '{}' AS effect_instance_key
        FROM jsonb_array_elements(db_inventory) AS database_item(value)
      UNION
      SELECT e.effect_instance_key FROM okg.external_action_restore_checkpoint_entries e
       WHERE e.deployment_name=bound_deployment AND e.restore_id=session_row.restore_id
         AND e.checkpoint_payload->>'proof_kind'='positive'
    ) inventory_union;
  derived_inventory_count:=jsonb_array_length(derived_inventory);
  derived_inventory_digest:=okg.external_action_restore_inventory_digest(derived_inventory);
  IF derived_inventory_count<>attestation_row.effect_inventory_count
     OR derived_inventory_digest<>attestation_row.effect_inventory_digest
     OR count_value<>derived_inventory_count
     OR keys IS DISTINCT FROM derived_inventory THEN
    RAISE EXCEPTION 'restore inventory is not the database plus positive-ledger union' USING ERRCODE='40001';
  END IF;
  RETURN jsonb_build_object(
    'deployment',bound_deployment,'restore_id',session_row.restore_id,
    'restore_attestation_envelope_digest',attestation_row.envelope_digest,
    'effect_inventory_digest',attestation_row.effect_inventory_digest,
    'effect_inventory_count',attestation_row.effect_inventory_count,
    'worker_checkpoint_set_digest',attestation_row.worker_checkpoint_set_digest,
    'worker_checkpoint_count',attestation_row.worker_checkpoint_count,
    'pre_restore_event_root_digest',attestation_row.pre_restore_event_root_digest,
    'pre_restore_event_head_count',attestation_row.pre_restore_event_head_count
  );
END
$external_action_restore_assert_coverage$;

-- Immutable registration facts; no execution or budget authority.

CREATE TABLE IF NOT EXISTS okg.external_automation_runs (
  record_id TEXT PRIMARY KEY,
  deployment_name TEXT NOT NULL,
  owner_separation_subject TEXT NOT NULL,
  run_id TEXT NOT NULL,
  automation_id TEXT NOT NULL,
  automation_version TEXT NOT NULL,
  correlation_id TEXT NOT NULL,
  reported_distribution JSONB NOT NULL,
  generation_id TEXT COLLATE "C" NOT NULL,
  registration_idempotency_key_hash TEXT NOT NULL,
  request_hash TEXT NOT NULL,
  authentication_audit_id TEXT NOT NULL,
  nomos_decision_id TEXT NOT NULL,
  registered_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  UNIQUE (deployment_name, record_id),
  UNIQUE (deployment_name, owner_separation_subject, run_id),
  UNIQUE (deployment_name, owner_separation_subject, registration_idempotency_key_hash)
);
CREATE TABLE IF NOT EXISTS okg.external_automation_run_proposals (
  deployment_name TEXT NOT NULL,
  record_id TEXT NOT NULL,
  proposal_id TEXT NOT NULL,
  owner_separation_subject TEXT NOT NULL,
  link_idempotency_key_hash TEXT NOT NULL,
  request_hash TEXT NOT NULL,
  authentication_audit_id TEXT NOT NULL,
  nomos_decision_id TEXT NOT NULL,
  proposal_read_authentication_audit_id TEXT NOT NULL,
  proposal_read_nomos_decision_id TEXT NOT NULL,
  linked_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (deployment_name, proposal_id),
  UNIQUE (record_id, owner_separation_subject, link_idempotency_key_hash),
  FOREIGN KEY (deployment_name, record_id)
    REFERENCES okg.external_automation_runs(deployment_name, record_id),
  FOREIGN KEY (deployment_name, proposal_id)
    REFERENCES okg.external_action_proposals(deployment_name, proposal_id)
);
ALTER TABLE okg.external_automation_runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE okg.external_automation_run_proposals ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS external_automation_runs_deployment ON okg.external_automation_runs;
CREATE POLICY external_automation_runs_deployment ON okg.external_automation_runs
  TO okg_external_action_function_owner
  USING (deployment_name = okg.external_action_session_deployment())
  WITH CHECK (deployment_name = okg.external_action_session_deployment());
DROP POLICY IF EXISTS external_automation_run_proposals_deployment ON okg.external_automation_run_proposals;
CREATE POLICY external_automation_run_proposals_deployment ON okg.external_automation_run_proposals
  TO okg_external_action_function_owner
  USING (deployment_name = okg.external_action_session_deployment())
  WITH CHECK (deployment_name = okg.external_action_session_deployment());

CREATE OR REPLACE FUNCTION okg.external_automation_transaction_context()
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, okg AS $run_tx$
BEGIN
  PERFORM okg.external_action_verifier_capability_status();
  PERFORM okg.external_action_session_deployment();
  RETURN jsonb_build_object('transaction_id', pg_current_xact_id()::text);
END $run_tx$;

-- Internal helper: no API-role EXECUTE grant. Every semantic RPC calls it.
CREATE OR REPLACE FUNCTION okg.external_automation_authority(
  p_command JSONB, p_operation TEXT, p_prefix TEXT
) RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, okg AS $run_authority$
DECLARE
  dep TEXT := okg.external_action_session_deployment();
  target_value JSONB;
  digest_value TEXT;
  snapshot JSONB;
  decision_target JSONB;
  decision_resource JSONB;
  proposal_value TEXT := p_command->'body'->>'proposal_id';
BEGIN
  PERFORM okg.external_action_verifier_capability_status();
  IF p_command->>'deployment' IS DISTINCT FROM dep
     OR p_command->>'owner' IS NULL OR p_command->>'record_id' IS NULL
     OR p_command->>'request_hash' IS NULL THEN
    RAISE EXCEPTION 'run_authority_invalid' USING ERRCODE = '42501';
  END IF;
  target_value := jsonb_build_object(
    'kind', CASE WHEN p_operation = 'external_action.read' THEN 'external_action' ELSE 'external_automation_run' END,
    'id', CASE WHEN p_operation = 'external_action.read' THEN proposal_value ELSE p_command->>'record_id' END,
    'operation', p_operation
  );
  digest_value := okg.external_action_hash('okg-external-automation-authorization-v1', jsonb_build_object(
    'transaction_id', pg_current_xact_id()::text, 'deployment', dep,
    'owner', p_command->>'owner', 'record_id', p_command->>'record_id',
    'proposal_id', proposal_value, 'request_hash', p_command->>'request_hash',
    'operation', p_operation, 'target', target_value
  ));
  snapshot := okg.external_action_nomos_authority_snapshot(
    dep, p_command->>(p_prefix || '_authentication_audit_id'),
    p_command->>(p_prefix || '_nomos_decision_id'), p_operation, digest_value
  );
  SELECT target, resource INTO decision_target, decision_resource FROM okg.nomos_decisions
    WHERE deployment_name = dep AND decision_id = p_command->>(p_prefix || '_nomos_decision_id');
  IF snapshot->>'separation_subject' IS DISTINCT FROM p_command->>'owner'
     OR decision_target IS DISTINCT FROM target_value
     OR decision_resource IS DISTINCT FROM (target_value || jsonb_build_object('resource_kind', target_value->>'kind', 'nomos_surface', p_operation, 'deployment', dep, 'scope', 'external_action')) THEN
    RAISE EXCEPTION 'run_authority_invalid' USING ERRCODE = '42501';
  END IF;
  RETURN snapshot;
END $run_authority$;

CREATE OR REPLACE FUNCTION okg.external_automation_projection(p_record_id TEXT)
RETURNS JSONB LANGUAGE sql SECURITY DEFINER
SET search_path = pg_catalog, okg AS $run_projection$
  SELECT jsonb_build_object(
    'record_id', r.record_id, 'deployment', r.deployment_name,
    'owner', r.owner_separation_subject, 'run_id', r.run_id,
    'automation_id', r.automation_id, 'automation_version', r.automation_version,
    'correlation_id', r.correlation_id,
    'reported_distribution', r.reported_distribution || '{"verification":"caller_reported"}'::jsonb,
    'generation', jsonb_build_object('generation_id', r.generation_id, 'verification', 'reference_verified_at_registration'),
    'registered_at', r.registered_at,
    'proposals', COALESCE((SELECT jsonb_agg(jsonb_build_object('proposal_id', l.proposal_id, 'linked_at', l.linked_at) ORDER BY l.proposal_id)
      FROM okg.external_automation_run_proposals l WHERE l.deployment_name = r.deployment_name AND l.record_id = r.record_id), '[]'::jsonb)
  ) FROM okg.external_automation_runs r
  WHERE r.record_id = p_record_id AND r.deployment_name = okg.external_action_session_deployment()
$run_projection$;

CREATE OR REPLACE FUNCTION okg.external_automation_register(p_command JSONB)
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, okg AS $run_register$
DECLARE
  dep TEXT := okg.external_action_session_deployment();
  b JSONB := p_command->'body';
  distribution JSONB := b->'reported_distribution';
  prior okg.external_automation_runs%ROWTYPE;
  field_name TEXT;
  key_hash TEXT := p_command->>'idempotency_key_hash';
  expected_hash TEXT;
BEGIN
  PERFORM okg.external_action_require_command(p_command,
    ARRAY['deployment','owner','record_id','request_hash','idempotency_key_hash','body','primary_authentication_audit_id','primary_nomos_decision_id'],
    ARRAY['deployment','owner','record_id','request_hash','idempotency_key_hash','body','primary_authentication_audit_id','primary_nomos_decision_id']);
  PERFORM okg.external_automation_authority(p_command, 'external_automation.register', 'primary');
  PERFORM okg.external_action_require_command(b,
    ARRAY['run_id','automation_id','automation_version','correlation_id','reported_distribution','generation_id'],
    ARRAY['run_id','automation_id','automation_version','correlation_id','reported_distribution','generation_id']);
  PERFORM okg.external_action_require_command(distribution,
    ARRAY['name','version','package_digest'], ARRAY['name','version']);
  IF NOT (distribution ? 'package_digest') THEN RAISE EXCEPTION 'invalid_run_record' USING ERRCODE = '22023'; END IF;
  FOREACH field_name IN ARRAY ARRAY['run_id','automation_id','automation_version','correlation_id'] LOOP
    IF jsonb_typeof(b->field_name) IS DISTINCT FROM 'string'
       OR b->>field_name !~ '^[A-Za-z0-9._:+-]{1,128}$'
       OR (field_name = 'automation_version' AND length(b->>field_name) > 64) THEN
      RAISE EXCEPTION 'invalid_run_record' USING ERRCODE = '22023';
    END IF;
  END LOOP;
  IF jsonb_typeof(distribution->'name') IS DISTINCT FROM 'string'
     OR jsonb_typeof(distribution->'version') IS DISTINCT FROM 'string'
     OR distribution->>'name' !~ '^[A-Za-z0-9._:+-]{1,128}$'
     OR distribution->>'version' !~ '^[A-Za-z0-9._:+-]{1,128}$'
     OR (distribution->'package_digest' <> 'null'::jsonb AND (jsonb_typeof(distribution->'package_digest') <> 'string' OR distribution->>'package_digest' !~ '^sha256:[0-9a-f]{64}$'))
     OR jsonb_typeof(b->'generation_id') IS DISTINCT FROM 'string'
     OR octet_length(b->>'generation_id') NOT BETWEEN 1 AND 1024
     OR b->>'generation_id' ~ '[[:cntrl:]]'
     OR p_command->>'record_id' !~ '^[0-9A-HJKMNP-TV-Z]{26}$'
     OR key_hash !~ '^sha256:[0-9a-f]{64}$' THEN
    RAISE EXCEPTION 'invalid_run_record' USING ERRCODE = '22023';
  END IF;
  expected_hash := okg.external_action_hash('okg-external-automation-request-v1', jsonb_build_object(
    'operation','register','deployment',dep,'owner',p_command->>'owner','record_id',NULL,'body',b));
  IF expected_hash IS DISTINCT FROM p_command->>'request_hash' THEN
    RAISE EXCEPTION 'run_authority_invalid' USING ERRCODE = '42501';
  END IF;
  -- Authority snapshot holds the deployment advisory lock, serializing keys.
  SELECT * INTO prior FROM okg.external_automation_runs
    WHERE deployment_name = dep AND owner_separation_subject = p_command->>'owner'
      AND registration_idempotency_key_hash = key_hash;
  IF FOUND THEN
    IF prior.request_hash <> expected_hash THEN
      RAISE EXCEPTION 'run_identity_conflict' USING ERRCODE = '40001';
    END IF;
    RETURN jsonb_build_object('status',200,'body',okg.external_automation_projection(prior.record_id));
  END IF;
  IF NOT EXISTS (SELECT 1 FROM okg.external_action_restore_fences WHERE deployment_name = dep AND service_mode = 'normal') THEN
    RAISE EXCEPTION 'run_restore_quarantined' USING ERRCODE = '55000';
  END IF;
  IF EXISTS (SELECT 1 FROM okg.external_automation_runs WHERE deployment_name = dep AND owner_separation_subject = p_command->>'owner' AND run_id = b->>'run_id') THEN
    RAISE EXCEPTION 'run_identity_conflict' USING ERRCODE = '40001';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM okg.v_generations WHERE id COLLATE "C" = (b->>'generation_id') COLLATE "C" AND deployment_name = dep) THEN
    RAISE EXCEPTION 'run_generation_invalid' USING ERRCODE = '22023';
  END IF;
  INSERT INTO okg.external_automation_runs(record_id,deployment_name,owner_separation_subject,run_id,automation_id,automation_version,correlation_id,reported_distribution,generation_id,registration_idempotency_key_hash,request_hash,authentication_audit_id,nomos_decision_id)
  VALUES(p_command->>'record_id',dep,p_command->>'owner',b->>'run_id',b->>'automation_id',b->>'automation_version',b->>'correlation_id',distribution,b->>'generation_id',key_hash,expected_hash,p_command->>'primary_authentication_audit_id',p_command->>'primary_nomos_decision_id');
  RETURN jsonb_build_object('status',201,'body',okg.external_automation_projection(p_command->>'record_id'));
END $run_register$;

CREATE OR REPLACE FUNCTION okg.external_automation_read(p_command JSONB)
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, okg AS $run_read$
DECLARE dep TEXT := okg.external_action_session_deployment();
BEGIN
  PERFORM okg.external_action_require_command(p_command,
    ARRAY['deployment','owner','record_id','request_hash','idempotency_key_hash','body','primary_authentication_audit_id','primary_nomos_decision_id'],
    ARRAY['deployment','owner','record_id','request_hash','body','primary_authentication_audit_id','primary_nomos_decision_id']);
  PERFORM okg.external_automation_authority(p_command, 'external_automation.read', 'primary');
  IF p_command->'body' IS DISTINCT FROM '{}'::jsonb OR p_command->>'request_hash' IS DISTINCT FROM
    okg.external_action_hash('okg-external-automation-request-v1', jsonb_build_object('operation','read','deployment',dep,'owner',p_command->>'owner','record_id',p_command->>'record_id','body','{}'::jsonb)) THEN
    RAISE EXCEPTION 'invalid_run_record' USING ERRCODE = '22023';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM okg.external_automation_runs WHERE deployment_name = dep AND record_id = p_command->>'record_id' AND owner_separation_subject = p_command->>'owner') THEN
    RAISE EXCEPTION 'run_not_found' USING ERRCODE = 'P0002';
  END IF;
  RETURN jsonb_build_object('status',200,'body',okg.external_automation_projection(p_command->>'record_id'));
END $run_read$;

CREATE OR REPLACE FUNCTION okg.external_automation_link(p_command JSONB)
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, okg AS $run_link$
DECLARE
  dep TEXT := okg.external_action_session_deployment();
  first_auth JSONB; second_auth JSONB;
  r okg.external_automation_runs%ROWTYPE;
  p okg.external_action_proposals%ROWTYPE;
  prior okg.external_automation_run_proposals%ROWTYPE;
  expected_hash TEXT;
  field_name TEXT;
BEGIN
  PERFORM okg.external_action_require_command(p_command,
    ARRAY['deployment','owner','record_id','request_hash','idempotency_key_hash','body','primary_authentication_audit_id','primary_nomos_decision_id','proposal_read_authentication_audit_id','proposal_read_nomos_decision_id'],
    ARRAY['deployment','owner','record_id','request_hash','idempotency_key_hash','body','primary_authentication_audit_id','primary_nomos_decision_id','proposal_read_authentication_audit_id','proposal_read_nomos_decision_id']);
  first_auth := okg.external_automation_authority(p_command, 'external_automation.link', 'primary');
  second_auth := okg.external_automation_authority(p_command, 'external_action.read', 'proposal_read');
  FOREACH field_name IN ARRAY ARRAY['deployment','principal','separation_subject','nomos_provider_id','nomos_subject_binding_id'] LOOP
    IF first_auth->field_name IS DISTINCT FROM second_auth->field_name THEN
      RAISE EXCEPTION 'run_authority_invalid' USING ERRCODE = '42501';
    END IF;
  END LOOP;
  IF p_command->>'primary_authentication_audit_id' = p_command->>'proposal_read_authentication_audit_id'
     OR p_command->>'primary_nomos_decision_id' = p_command->>'proposal_read_nomos_decision_id' THEN
    RAISE EXCEPTION 'run_authority_invalid' USING ERRCODE = '42501';
  END IF;
  PERFORM okg.external_action_require_command(p_command->'body', ARRAY['proposal_id'], ARRAY['proposal_id']);
  IF jsonb_typeof(p_command->'body'->'proposal_id') IS DISTINCT FROM 'string'
     OR p_command->'body'->>'proposal_id' !~ '^[0-9A-HJKMNP-TV-Z]{26}$'
     OR p_command->>'idempotency_key_hash' !~ '^sha256:[0-9a-f]{64}$' THEN
    RAISE EXCEPTION 'invalid_run_record' USING ERRCODE = '22023';
  END IF;
  expected_hash := okg.external_action_hash('okg-external-automation-request-v1', jsonb_build_object('operation','link','deployment',dep,'owner',p_command->>'owner','record_id',p_command->>'record_id','body',p_command->'body'));
  IF expected_hash IS DISTINCT FROM p_command->>'request_hash' THEN
    RAISE EXCEPTION 'run_authority_invalid' USING ERRCODE = '42501';
  END IF;
  -- First row lookup is after BOTH permissions, including on a replay.
  SELECT * INTO r FROM okg.external_automation_runs WHERE deployment_name = dep AND record_id = p_command->>'record_id' AND owner_separation_subject = p_command->>'owner';
  IF NOT FOUND THEN RAISE EXCEPTION 'run_not_found' USING ERRCODE = 'P0002'; END IF;
  SELECT * INTO p FROM okg.external_action_proposals WHERE deployment_name = dep AND proposal_id = p_command->'body'->>'proposal_id' FOR SHARE;
  IF NOT FOUND OR p.proposer_separation_subject <> r.owner_separation_subject
     OR p.automation_id <> r.automation_id OR p.automation_version <> r.automation_version
     OR p.run_id <> r.run_id OR p.correlation_id <> r.correlation_id THEN
    RAISE EXCEPTION 'run_proposal_mismatch' USING ERRCODE = '40001';
  END IF;
  SELECT * INTO prior FROM okg.external_automation_run_proposals WHERE record_id = r.record_id AND owner_separation_subject = r.owner_separation_subject AND link_idempotency_key_hash = p_command->>'idempotency_key_hash';
  IF FOUND THEN
    IF prior.request_hash <> expected_hash THEN RAISE EXCEPTION 'run_identity_conflict' USING ERRCODE = '40001'; END IF;
    RETURN jsonb_build_object('status',200,'body',jsonb_build_object('record_id',r.record_id,'proposal_id',prior.proposal_id,'linked_at',prior.linked_at));
  END IF;
  IF NOT EXISTS (SELECT 1 FROM okg.external_action_restore_fences WHERE deployment_name = dep AND service_mode = 'normal') THEN RAISE EXCEPTION 'run_restore_quarantined' USING ERRCODE = '55000'; END IF;
  IF EXISTS (SELECT 1 FROM okg.external_automation_run_proposals WHERE deployment_name = dep AND proposal_id = p.proposal_id) THEN RAISE EXCEPTION 'run_identity_conflict' USING ERRCODE = '40001'; END IF;
  IF (SELECT count(*) FROM okg.external_automation_run_proposals WHERE record_id = r.record_id) >= 64 THEN RAISE EXCEPTION 'run_link_limit' USING ERRCODE = '54000'; END IF;
  INSERT INTO okg.external_automation_run_proposals(deployment_name,record_id,proposal_id,owner_separation_subject,link_idempotency_key_hash,request_hash,authentication_audit_id,nomos_decision_id,proposal_read_authentication_audit_id,proposal_read_nomos_decision_id)
    VALUES(dep,r.record_id,p.proposal_id,r.owner_separation_subject,p_command->>'idempotency_key_hash',expected_hash,p_command->>'primary_authentication_audit_id',p_command->>'primary_nomos_decision_id',p_command->>'proposal_read_authentication_audit_id',p_command->>'proposal_read_nomos_decision_id') RETURNING * INTO prior;
  RETURN jsonb_build_object('status',201,'body',jsonb_build_object('record_id',r.record_id,'proposal_id',prior.proposal_id,'linked_at',prior.linked_at));
END $run_link$;

-- BEGIN external automation run grants (also converged by views.sql).
REVOKE ALL ON okg.external_automation_runs, okg.external_automation_run_proposals FROM PUBLIC, okg_external_action_api;
GRANT SELECT, INSERT ON okg.external_automation_runs, okg.external_automation_run_proposals TO okg_external_action_function_owner;
DO $run_generation_grant$
BEGIN
  -- Fresh schema precedes views.sql, which unconditionally converges this
  -- grant after creating the authoritative view. Upgrades already have it.
  IF to_regclass('okg.v_generations') IS NOT NULL THEN
    GRANT SELECT (id, deployment_name) ON okg.v_generations TO okg_external_action_function_owner;
  END IF;
END $run_generation_grant$;
REVOKE ALL ON FUNCTION okg.external_automation_transaction_context(), okg.external_automation_register(JSONB), okg.external_automation_read(JSONB), okg.external_automation_link(JSONB), okg.external_automation_authority(JSONB,TEXT,TEXT), okg.external_automation_projection(TEXT) FROM PUBLIC, okg_external_action_api;
DO $run_read_role_revoke$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_ro') THEN
    REVOKE ALL ON FUNCTION okg.external_automation_transaction_context(), okg.external_automation_register(JSONB), okg.external_automation_read(JSONB), okg.external_automation_link(JSONB), okg.external_automation_authority(JSONB,TEXT,TEXT), okg.external_automation_projection(TEXT) FROM app_ro;
  END IF;
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'okg_mcp') THEN
    REVOKE ALL ON FUNCTION okg.external_automation_transaction_context(), okg.external_automation_register(JSONB), okg.external_automation_read(JSONB), okg.external_automation_link(JSONB), okg.external_automation_authority(JSONB,TEXT,TEXT), okg.external_automation_projection(TEXT) FROM okg_mcp;
  END IF;
END $run_read_role_revoke$;
ALTER FUNCTION okg.external_automation_transaction_context() OWNER TO okg_external_action_function_owner;
ALTER FUNCTION okg.external_automation_register(JSONB) OWNER TO okg_external_action_function_owner;
ALTER FUNCTION okg.external_automation_read(JSONB) OWNER TO okg_external_action_function_owner;
ALTER FUNCTION okg.external_automation_link(JSONB) OWNER TO okg_external_action_function_owner;
ALTER FUNCTION okg.external_automation_authority(JSONB,TEXT,TEXT) OWNER TO okg_external_action_function_owner;
ALTER FUNCTION okg.external_automation_projection(TEXT) OWNER TO okg_external_action_function_owner;
GRANT EXECUTE ON FUNCTION okg.external_automation_transaction_context(), okg.external_automation_register(JSONB), okg.external_automation_read(JSONB), okg.external_automation_link(JSONB) TO okg_external_action_api;
GRANT EXECUTE ON FUNCTION okg.external_automation_transaction_context(), okg.external_automation_register(JSONB), okg.external_automation_read(JSONB), okg.external_automation_link(JSONB), okg.external_automation_authority(JSONB,TEXT,TEXT), okg.external_automation_projection(TEXT) TO okg_external_action_function_owner;
-- END external automation run grants.

-- ========== Vector sets (PACT openrouter-embeddings-hnsw, design v3.2) ==========
--
-- Vectors live outside the versioned node rows. A deployment declares named
-- vector sets in deployment.yaml (embeddings.sets); `okg migrate` registers
-- each one here under its identity hash (model, width, cut, prefixes,
-- provider, document format version). The old node columns
-- okg.graph_nodes.embedding / embedding_model are untouched by these tables.
--
-- next_seq is the per-set counter of document links: a writer takes its
-- numbers with UPDATE ... SET next_seq = next_seq + n RETURNING inside its
-- own insert transaction, so the row lock makes one set's link writers
-- commit in number order, and the publisher's committed read of
-- next_seq - 1 is a safe watermark. Vectors have their own counter
-- (okg.embedding_vector_counters): a backfill holds it through its HNSW
-- inserts, and graph writers, which write links only, never wait on it.
CREATE TABLE IF NOT EXISTS okg.embedding_sets (
  set_id          TEXT        PRIMARY KEY,
  name            TEXT        NOT NULL,
  backend         TEXT        NOT NULL,
  identity_json   JSONB       NOT NULL,
  width           INTEGER     NOT NULL,
  documents_json  JSONB       NOT NULL,
  index_json      JSONB       NOT NULL,
  next_seq        BIGINT      NOT NULL DEFAULT 1,
  -- FALSE once `okg migrate` finds the set gone from deployment.yaml: it
  -- keeps its rows (old pins read them) and takes no new links.
  active          BOOLEAN     NOT NULL DEFAULT TRUE,
  registered_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT embedding_sets_id_shape CHECK (set_id ~ '^es_[0-9a-f]{24}$'),
  CONSTRAINT embedding_sets_width_range CHECK (width BETWEEN 1 AND 16000),
  CONSTRAINT embedding_sets_next_seq_positive CHECK (next_seq >= 1)
);

-- One vector per (set, document hash). Insert-only (ON CONFLICT DO NOTHING):
-- identical documents embed once, and a row is never rewritten. Each set's
-- HNSW index is partial on set_id and built by `okg migrate` /
-- embeddings.store.ensure_set_index under a name derived from the set id.
-- A set stores halfvec (the default) in `embedding` or, with `storage:
-- vector` (PACT vector-set-pgvector-widths), full-precision vector in
-- `embedding_f32`; exactly one of the two is set on a row.
CREATE TABLE IF NOT EXISTS okg.text_embeddings (
  set_id         TEXT        NOT NULL REFERENCES okg.embedding_sets (set_id),
  doc_hash       TEXT        NOT NULL,
  embedding      halfvec,
  seq            BIGINT      NOT NULL,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  -- Last, as on a table the ALTER below upgraded.
  embedding_f32  vector,
  PRIMARY KEY (set_id, doc_hash),
  CONSTRAINT text_embeddings_doc_hash_shape CHECK (doc_hash ~ '^[0-9a-f]{64}$'),
  CONSTRAINT text_embeddings_seq_positive CHECK (seq >= 1),
  CONSTRAINT text_embeddings_one_vector CHECK (num_nonnulls(embedding, embedding_f32) = 1)
);
-- A table created before the storage choice: add the vector column, let
-- `embedding` be empty on a vector set's row, and add the one-vector check.
-- Every existing row has `embedding` set and no `embedding_f32`, so the
-- check holds for them. `okg migrate` runs this file in one transaction,
-- so these ALTERs take an ACCESS EXCLUSIVE lock held until commit, and
-- adding the check scans every row under it: reads and writes of
-- okg.text_embeddings wait for the scan. That happens once per database.
-- Each ALTER runs only when the catalog shows its step is still missing
-- (ADD COLUMN IF NOT EXISTS alone would lock before it checks), so a
-- rerun of this file on an upgraded table takes no exclusive lock here. A
-- short exclusive lock that grows with table size: on 428k rows (1.7 GB),
-- measured on the earlier two-step NOT VALID then VALIDATE form, migrate
-- held it about 2.4 s and the scan took 0.6 s warm, 3.05 s cold. The check is added
-- validated, so it is enforced on every existing row at once.
DO $text_embeddings_one_vector$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_attribute
     WHERE attrelid = 'okg.text_embeddings'::regclass
       AND attname = 'embedding_f32' AND NOT attisdropped
  ) THEN
    ALTER TABLE okg.text_embeddings ADD COLUMN embedding_f32 vector;
  END IF;
  IF EXISTS (
    SELECT 1 FROM pg_attribute
     WHERE attrelid = 'okg.text_embeddings'::regclass
       AND attname = 'embedding' AND attnotnull
  ) THEN
    ALTER TABLE okg.text_embeddings ALTER COLUMN embedding DROP NOT NULL;
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conrelid = 'okg.text_embeddings'::regclass
       AND conname = 'text_embeddings_one_vector'
  ) THEN
    ALTER TABLE okg.text_embeddings
      ADD CONSTRAINT text_embeddings_one_vector
      CHECK (num_nonnulls(embedding, embedding_f32) = 1);
  END IF;
END $text_embeddings_one_vector$;

-- The per-set counter of vectors, taken like next_seq above. A separate row
-- (a row lock covers every column of its row), so a backfill's vector
-- batch, which holds the lock through its HNSW inserts, never blocks a
-- graph writer's links. A set registered when links and vectors shared one
-- counter starts here at that counter's value, so a watermark recorded then
-- (max_vector_seq NULL: max_seq bounds both) never covers a later vector.
-- `okg migrate` (embeddings.store.register_sets) adds the row of a new set.
CREATE TABLE IF NOT EXISTS okg.embedding_vector_counters (
  set_id      TEXT    PRIMARY KEY REFERENCES okg.embedding_sets (set_id),
  next_seq    BIGINT  NOT NULL DEFAULT 1,
  CONSTRAINT embedding_vector_counters_next_seq_positive CHECK (next_seq >= 1)
);
INSERT INTO okg.embedding_vector_counters (set_id, next_seq)
SELECT s.set_id,
       GREATEST(s.next_seq,
                COALESCE((SELECT max(e.seq) + 1 FROM okg.text_embeddings AS e
                           WHERE e.set_id = s.set_id), 1))
  FROM okg.embedding_sets AS s
ON CONFLICT (set_id) DO NOTHING;

-- Which document a node's content renders to, per set and template.
-- row_hash = sha256(subtype || attrs::text) of the node row, computed by
-- PostgreSQL, so a link names content, never a Chronos row position (an
-- in-place rewrite inside a segment keeps live_lo with new content). The
-- graph writer adds rows in the node-upsert path of its own transaction.
-- Insert-only. seq is in the key: when a template becomes current again
-- (T1, T2, then T1) a writer adds a newer copy of the T1 link, so the
-- reader's "newest link wins" picks it while older pins keep the old rows.
CREATE TABLE IF NOT EXISTS okg.node_documents (
  set_id         TEXT        NOT NULL REFERENCES okg.embedding_sets (set_id),
  template_hash  TEXT        NOT NULL,
  node_id        TEXT        NOT NULL,
  row_hash       TEXT        NOT NULL,
  doc_hash       TEXT        NOT NULL,
  seq            BIGINT      NOT NULL,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (set_id, template_hash, node_id, row_hash, seq),
  CONSTRAINT node_documents_hash_shape CHECK (
    template_hash ~ '^[0-9a-f]{64}$'
    AND row_hash ~ '^[0-9a-f]{64}$'
    AND doc_hash ~ '^[0-9a-f]{64}$'
  ),
  CONSTRAINT node_documents_seq_positive CHECK (seq >= 1)
);
-- A table created by an earlier build of this change has the key without
-- seq; widen it.
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_constraint c
     WHERE c.conrelid = 'okg.node_documents'::regclass
       AND c.conname = 'node_documents_pkey'
       AND cardinality(c.conkey) = 4
  ) THEN
    ALTER TABLE okg.node_documents
      DROP CONSTRAINT node_documents_pkey,
      ADD CONSTRAINT node_documents_pkey PRIMARY KEY
        (set_id, template_hash, node_id, row_hash, seq);
  END IF;
END $$;
CREATE INDEX IF NOT EXISTS node_documents_doc_hash_idx
  ON okg.node_documents (set_id, doc_hash);
-- A node's links under every template, newest first, for the reader's
-- "newest servable link" choice.
CREATE INDEX IF NOT EXISTS node_documents_node_idx
  ON okg.node_documents (set_id, node_id, row_hash, seq);
-- The search route asks whether a pin sees any vector of a set
-- (seq <= watermark); without this it reads every row of the set while a
-- backfill waits for its publish.
CREATE INDEX IF NOT EXISTS text_embeddings_set_seq_idx
  ON okg.text_embeddings (set_id, seq);

-- What each template hash a link names was built from: the template, and
-- the read class of each header field when the documents were built
-- (embeddings.documents.template_record). A template change keeps the set
-- id, so a reader serves a node's newest link whose template is still safe
-- under today's classification (a header field that became maskable fails
-- closed); this table is how it knows. Written with the links. Insert-only.
CREATE TABLE IF NOT EXISTS okg.embedding_templates (
  set_id         TEXT        NOT NULL REFERENCES okg.embedding_sets (set_id),
  template_hash  TEXT        NOT NULL,
  subtype        TEXT        NOT NULL,
  record_json    JSONB       NOT NULL,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (set_id, template_hash),
  CONSTRAINT embedding_templates_hash_shape CHECK (
    template_hash ~ '^[0-9a-f]{64}$'
  )
);

-- The highest seq a published generation may see, per set. Written by the
-- publisher inside the publish transaction (recorded_by 'publisher'), or
-- once by `okg migrate` for the generation current when a set is registered
-- (recorded_by 'migrate'), so search works right after the migration. A
-- generation with no row for a set has no vectors from it. text_nodes and
-- embedded_nodes are NULL until the coverage count is computed (later step).
CREATE TABLE IF NOT EXISTS okg.embedding_watermarks (
  generation_id   TEXT        NOT NULL,
  set_id          TEXT        NOT NULL REFERENCES okg.embedding_sets (set_id),
  max_seq         BIGINT      NOT NULL,
  text_nodes      BIGINT,
  embedded_nodes  BIGINT,
  recorded_by     TEXT        NOT NULL,
  recorded_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (generation_id, set_id),
  CONSTRAINT embedding_watermarks_max_seq_nonnegative CHECK (max_seq >= 0),
  CONSTRAINT embedding_watermarks_recorded_by_known CHECK (
    recorded_by IN ('publisher', 'migrate')
  )
);
-- max_seq bounds the links, max_vector_seq the vectors (their own counter).
-- NULL on a row recorded while both shared one counter: max_seq bounds both.
ALTER TABLE okg.embedding_watermarks
  ADD COLUMN IF NOT EXISTS max_vector_seq BIGINT;

-- One row per backfill of a set that COMPLETED with no document left
-- without a vector at its pin (a --limit, cap or failed run writes none).
-- The counters' committed values when it finished; max_seq is -1 when
-- another writer took links during the fill (never carried to a later
-- generation without a full pass). Insert-only.
-- Jason, lane chat 2026-10-14, "ehroute yes": semantic search routes to a
-- set only after such a completion, so a partial fill never flips it.
CREATE TABLE IF NOT EXISTS okg.embedding_fill_completions (
  set_id          TEXT        NOT NULL REFERENCES okg.embedding_sets (set_id),
  generation_id   TEXT        NOT NULL,
  max_seq         BIGINT      NOT NULL,
  max_vector_seq  BIGINT      NOT NULL,
  completed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (set_id, generation_id)
);
-- The newest completion's max_vector_seq when the watermark was recorded,
-- or NULL (no completed fill yet: the read keeps the node columns). Pinned
-- with the watermark, so the route cannot flip between pages of one pin.
ALTER TABLE okg.embedding_watermarks
  ADD COLUMN IF NOT EXISTS filled_vector_seq BIGINT;

-- The runtime writer (graph writes, publish, backfill) inserts vectors, links
-- and watermarks and moves a set's counter; it can neither rewrite nor delete
-- a vector, link or watermark, so insert-only holds in the database too.
-- Registering a set (name, documents, index, active) is `okg migrate`'s, as
-- the owner. app_ro gets nothing here (default-deny; see views.sql); okg_mcp
-- reads these tables through grant-registry rows in views.sql.
GRANT SELECT, INSERT ON
  okg.text_embeddings, okg.node_documents, okg.embedding_watermarks,
  okg.embedding_templates
TO app_rw;
REVOKE UPDATE, DELETE, TRUNCATE ON
  okg.text_embeddings, okg.node_documents, okg.embedding_watermarks,
  okg.embedding_templates
FROM app_rw;
GRANT SELECT ON okg.embedding_sets TO app_rw;
REVOKE INSERT, DELETE, TRUNCATE ON okg.embedding_sets FROM app_rw;
GRANT UPDATE (next_seq, updated_at) ON okg.embedding_sets TO app_rw;
GRANT SELECT ON okg.embedding_vector_counters TO app_rw;
REVOKE INSERT, DELETE, TRUNCATE ON okg.embedding_vector_counters FROM app_rw;
GRANT UPDATE (next_seq) ON okg.embedding_vector_counters TO app_rw;
GRANT SELECT, INSERT ON okg.embedding_fill_completions TO app_rw;
REVOKE UPDATE, DELETE, TRUNCATE ON okg.embedding_fill_completions FROM app_rw;
