-- PACT bound-deployment-backup-latency:
-- constant-work integrity receipts for the interval-backed attribute read model.

CREATE TABLE IF NOT EXISTS okg.attribute_effective_integrity_state (
  deployment_name TEXT NOT NULL,
  branch_id TEXT NOT NULL,
  namespace TEXT NOT NULL,
  mutation_epoch BIGINT NOT NULL DEFAULT 0,
  clean_mutation_epoch BIGINT,
  dirty BOOLEAN NOT NULL DEFAULT TRUE,
  dirty_reason TEXT NOT NULL DEFAULT 'unanchored',
  latest_receipt_digest TEXT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (deployment_name, branch_id, namespace)
);

CREATE TABLE IF NOT EXISTS okg.attribute_effective_integrity_receipts (
  receipt_digest TEXT PRIMARY KEY,
  receipt_type TEXT NOT NULL CHECK (receipt_type IN ('anchor', 'incremental')),
  deployment_name TEXT NOT NULL,
  branch_id TEXT NOT NULL,
  namespace TEXT NOT NULL,
  generation_id TEXT NOT NULL,
  ordinal BIGINT NOT NULL,
  prior_receipt_digest TEXT,
  checkpoint_node_count BIGINT NOT NULL,
  checkpoint_edge_count BIGINT NOT NULL,
  logical_node_count BIGINT NOT NULL,
  logical_edge_count BIGINT NOT NULL,
  schema_version INTEGER NOT NULL,
  schema_digest TEXT NOT NULL,
  migration_digest TEXT NOT NULL,
  logical_payload_seed0 NUMERIC NOT NULL,
  logical_payload_seed1 NUMERIC NOT NULL,
  interval_payload_seed0 NUMERIC NOT NULL,
  interval_payload_seed1 NUMERIC NOT NULL,
  dirty_scope_digest TEXT NOT NULL,
  mutation_epoch BIGINT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  FOREIGN KEY (prior_receipt_digest)
    REFERENCES okg.attribute_effective_integrity_receipts(receipt_digest)
);

CREATE INDEX IF NOT EXISTS attribute_effective_integrity_receipt_scope_idx
  ON okg.attribute_effective_integrity_receipts
  (deployment_name, branch_id, namespace, ordinal DESC);

CREATE OR REPLACE FUNCTION okg.mark_attribute_effective_integrity_dirty()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $function$
BEGIN
  UPDATE okg.attribute_effective_integrity_state
     SET mutation_epoch = mutation_epoch + 1,
         dirty = TRUE,
         dirty_reason = TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME || ':' || TG_OP,
         updated_at = now();
  RETURN NULL;
END
$function$;

CREATE OR REPLACE FUNCTION okg.reject_attribute_integrity_receipt_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
BEGIN
  RAISE EXCEPTION 'attribute integrity receipts are append-only';
END
$function$;

DO $triggers$
DECLARE
  protected_table regclass;
  trigger_name text;
  operation text;
BEGIN
  FOREACH protected_table IN ARRAY ARRAY[
    'okg.attribute_effective_view'::regclass,
    'okg.attribute_effective_view_intervals'::regclass
  ] LOOP
    FOREACH operation IN ARRAY ARRAY['INSERT', 'UPDATE', 'DELETE', 'TRUNCATE'] LOOP
      trigger_name := 'attribute_integrity_dirty_' || lower(operation) ||
        '_' || substr(md5(protected_table::text), 1, 10);
      IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
         WHERE tgrelid = protected_table AND tgname = trigger_name
      ) THEN
        EXECUTE format(
          'CREATE TRIGGER %I AFTER %s ON %s FOR EACH STATEMENT '
          'EXECUTE FUNCTION okg.mark_attribute_effective_integrity_dirty()',
          trigger_name, operation, protected_table
        );
      END IF;
    END LOOP;
  END LOOP;
  IF NOT EXISTS (
    SELECT 1 FROM pg_trigger
     WHERE tgrelid = 'okg.attribute_effective_integrity_receipts'::regclass
       AND tgname = 'attribute_integrity_receipts_immutable'
  ) THEN
    CREATE TRIGGER attribute_integrity_receipts_immutable
      BEFORE UPDATE OR DELETE OR TRUNCATE
      ON okg.attribute_effective_integrity_receipts
      FOR EACH STATEMENT
      EXECUTE FUNCTION okg.reject_attribute_integrity_receipt_mutation();
  END IF;
END
$triggers$;

DO $privileges$
DECLARE role_name text;
BEGIN
  FOREACH role_name IN ARRAY ARRAY['app_rw', 'app_ro', 'okg_mcp'] LOOP
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role_name) THEN
      EXECUTE format(
        'REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON '
        'okg.attribute_effective_integrity_state, '
        'okg.attribute_effective_integrity_receipts FROM %I', role_name
      );
      EXECUTE format(
        'GRANT SELECT ON okg.attribute_effective_integrity_state, '
        'okg.attribute_effective_integrity_receipts TO %I', role_name
      );
    END IF;
  END LOOP;
END
$privileges$;
