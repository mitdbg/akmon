-- Opt-in bounded indexed source record authority (issue 1325).
--
-- Additive and idempotent.  schema.sql carries the same current-state
-- definitions for fresh installs; this file upgrades deployments that already
-- applied 0030_external_action_review.  Legacy okg.source_record_sets and
-- okg.source_record_set_blobs are untouched and remain the default format.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

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
