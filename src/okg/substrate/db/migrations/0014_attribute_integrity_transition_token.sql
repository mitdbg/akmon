-- PACT bound-deployment-backup-latency:
-- invalidate an in-flight publisher proof when another database session writes
-- a protected effective-attribute relation.  Authority is bound to PostgreSQL's
-- actual backend identity; it is not a caller-supplied or reusable bearer.

CREATE TABLE IF NOT EXISTS okg.attribute_effective_integrity_transitions (
  deployment_name TEXT NOT NULL,
  branch_id TEXT NOT NULL,
  namespace TEXT NOT NULL,
  backend_pid INTEGER NOT NULL,
  backend_start TIMESTAMPTZ NOT NULL,
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (deployment_name, branch_id, namespace)
);

CREATE OR REPLACE FUNCTION okg.begin_attribute_integrity_transition(
  p_deployment TEXT, p_branch TEXT, p_namespace TEXT
) RETURNS BOOLEAN
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $function$
BEGIN
  DELETE FROM okg.attribute_effective_integrity_transitions
   WHERE deployment_name = p_deployment AND branch_id = p_branch
     AND namespace = p_namespace;
  INSERT INTO okg.attribute_effective_integrity_transitions
    (deployment_name, branch_id, namespace, backend_pid, backend_start)
  SELECT p_deployment, p_branch, p_namespace, pg_backend_pid(), backend_start
    FROM pg_stat_activity WHERE pid = pg_backend_pid();
  IF NOT FOUND THEN
    RAISE EXCEPTION 'current backend identity is unavailable';
  END IF;
  RETURN TRUE;
END
$function$;

CREATE OR REPLACE FUNCTION okg.attribute_integrity_transition_matches(
  p_deployment TEXT, p_branch TEXT, p_namespace TEXT
) RETURNS BOOLEAN
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $function$
  SELECT EXISTS (
    SELECT 1 FROM okg.attribute_effective_integrity_transitions
     WHERE deployment_name = p_deployment AND branch_id = p_branch
       AND namespace = p_namespace
       AND backend_pid = pg_backend_pid()
       AND backend_start = (
         SELECT backend_start FROM pg_stat_activity WHERE pid = pg_backend_pid()
       )
  )
$function$;

CREATE OR REPLACE FUNCTION okg.finish_attribute_integrity_transition(
  p_deployment TEXT, p_branch TEXT, p_namespace TEXT
) RETURNS BOOLEAN
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $function$
DECLARE affected INTEGER;
BEGIN
  DELETE FROM okg.attribute_effective_integrity_transitions
   WHERE deployment_name = p_deployment AND branch_id = p_branch
     AND namespace = p_namespace
     AND backend_pid = pg_backend_pid()
     AND backend_start = (
       SELECT backend_start FROM pg_stat_activity WHERE pid = pg_backend_pid()
     );
  GET DIAGNOSTICS affected = ROW_COUNT;
  RETURN affected = 1;
END
$function$;

CREATE OR REPLACE FUNCTION okg.abort_attribute_integrity_transition(
  p_deployment TEXT, p_branch TEXT, p_namespace TEXT
) RETURNS VOID
LANGUAGE sql SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $function$
  DELETE FROM okg.attribute_effective_integrity_transitions
   WHERE deployment_name = p_deployment AND branch_id = p_branch
     AND namespace = p_namespace
     AND backend_pid = pg_backend_pid()
     AND backend_start = (
       SELECT backend_start FROM pg_stat_activity WHERE pid = pg_backend_pid()
     )
$function$;

CREATE OR REPLACE FUNCTION okg.register_attribute_integrity_scope(
  p_deployment TEXT, p_branch TEXT, p_namespace TEXT
) RETURNS VOID
LANGUAGE sql SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $function$
  INSERT INTO okg.attribute_effective_integrity_state
    (deployment_name, branch_id, namespace)
  VALUES (p_deployment, p_branch, p_namespace)
  ON CONFLICT (deployment_name, branch_id, namespace) DO NOTHING
$function$;

CREATE OR REPLACE FUNCTION okg.mark_attribute_integrity_scope_dirty(
  p_deployment TEXT, p_branch TEXT, p_namespace TEXT, p_reason TEXT
) RETURNS VOID
LANGUAGE sql SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $function$
  UPDATE okg.attribute_effective_integrity_state
     SET mutation_epoch = mutation_epoch + 1,
         dirty = TRUE,
         dirty_reason = p_reason,
         updated_at = now()
   WHERE deployment_name = p_deployment AND branch_id = p_branch
     AND namespace = p_namespace
$function$;

CREATE OR REPLACE FUNCTION okg.finalize_attribute_integrity_receipt(
  p_receipt_digest TEXT,
  p_receipt_type TEXT,
  p_deployment TEXT,
  p_branch TEXT,
  p_namespace TEXT,
  p_generation_id TEXT,
  p_ordinal BIGINT,
  p_prior_receipt_digest TEXT,
  p_checkpoint_node_count BIGINT,
  p_checkpoint_edge_count BIGINT,
  p_logical_node_count BIGINT,
  p_logical_edge_count BIGINT,
  p_schema_version INTEGER,
  p_schema_digest TEXT,
  p_migration_digest TEXT,
  p_logical_payload_seed0 NUMERIC,
  p_logical_payload_seed1 NUMERIC,
  p_interval_payload_seed0 NUMERIC,
  p_interval_payload_seed1 NUMERIC,
  p_dirty_scope_digest TEXT,
  p_mutation_epoch BIGINT
) RETURNS BOOLEAN
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $function$
BEGIN
  -- The calling backend must own the live transition token for this scope;
  -- finalization consumes it atomically with the receipt append and the
  -- clean-state compare-and-set on the mutation epoch.  This function is
  -- the only clean-marking authority: direct DML on the state and receipt
  -- tables is revoked from the runtime and application roles.
  IF NOT okg.finish_attribute_integrity_transition(
    p_deployment, p_branch, p_namespace
  ) THEN
    RAISE EXCEPTION
      'attribute integrity transition changed during finalization';
  END IF;
  INSERT INTO okg.attribute_effective_integrity_receipts (
    receipt_digest, receipt_type, deployment_name, branch_id,
    namespace, generation_id, ordinal, prior_receipt_digest,
    checkpoint_node_count, checkpoint_edge_count,
    logical_node_count, logical_edge_count,
    schema_version, schema_digest, migration_digest,
    logical_payload_seed0, logical_payload_seed1,
    interval_payload_seed0, interval_payload_seed1,
    dirty_scope_digest, mutation_epoch
  ) VALUES (
    p_receipt_digest, p_receipt_type, p_deployment, p_branch,
    p_namespace, p_generation_id, p_ordinal, p_prior_receipt_digest,
    p_checkpoint_node_count, p_checkpoint_edge_count,
    p_logical_node_count, p_logical_edge_count,
    p_schema_version, p_schema_digest, p_migration_digest,
    p_logical_payload_seed0, p_logical_payload_seed1,
    p_interval_payload_seed0, p_interval_payload_seed1,
    p_dirty_scope_digest, p_mutation_epoch
  );
  UPDATE okg.attribute_effective_integrity_state
     SET clean_mutation_epoch = p_mutation_epoch,
         dirty = FALSE,
         dirty_reason = 'clean',
         latest_receipt_digest = p_receipt_digest,
         updated_at = now()
   WHERE deployment_name = p_deployment AND branch_id = p_branch
     AND namespace = p_namespace
     AND mutation_epoch = p_mutation_epoch;
  IF NOT FOUND THEN
    RAISE EXCEPTION
      'attribute integrity transition changed during finalization';
  END IF;
  RETURN TRUE;
END
$function$;

UPDATE okg.attribute_effective_integrity_state
   SET mutation_epoch = mutation_epoch + 1,
       dirty = TRUE,
       dirty_reason = 'migration:0014_attribute_integrity_transition_token',
       updated_at = now();

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
  DELETE FROM okg.attribute_effective_integrity_transitions
   WHERE backend_pid <> pg_backend_pid()
      OR backend_start IS DISTINCT FROM (
        SELECT backend_start FROM pg_stat_activity WHERE pid = pg_backend_pid()
      );
  RETURN NULL;
END
$function$;

DO $privileges$
DECLARE role_name text;
BEGIN
  REVOKE ALL ON okg.attribute_effective_integrity_transitions FROM PUBLIC;
  REVOKE ALL ON FUNCTION
    okg.begin_attribute_integrity_transition(TEXT, TEXT, TEXT),
    okg.attribute_integrity_transition_matches(TEXT, TEXT, TEXT),
    okg.finish_attribute_integrity_transition(TEXT, TEXT, TEXT),
    okg.abort_attribute_integrity_transition(TEXT, TEXT, TEXT),
    okg.register_attribute_integrity_scope(TEXT, TEXT, TEXT),
    okg.mark_attribute_integrity_scope_dirty(TEXT, TEXT, TEXT, TEXT),
    okg.finalize_attribute_integrity_receipt(
      TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, BIGINT, TEXT,
      BIGINT, BIGINT, BIGINT, BIGINT, INTEGER, TEXT, TEXT,
      NUMERIC, NUMERIC, NUMERIC, NUMERIC, TEXT, BIGINT
    )
    FROM PUBLIC;
  FOREACH role_name IN ARRAY ARRAY[
    'okg_substrate', 'app_rw', 'app_ro', 'okg_mcp'
  ] LOOP
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role_name) THEN
      EXECUTE format(
        'REVOKE ALL ON okg.attribute_effective_integrity_transitions FROM %I',
        role_name
      );
      -- The restricted finalizer is the only clean-marking authority;
      -- direct DML on the state and receipt tables is not a runtime API.
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
  FOREACH role_name IN ARRAY ARRAY['app_ro', 'okg_mcp'] LOOP
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role_name) THEN
      EXECUTE format(
        'REVOKE ALL ON FUNCTION '
        'okg.begin_attribute_integrity_transition(TEXT, TEXT, TEXT), '
        'okg.attribute_integrity_transition_matches(TEXT, TEXT, TEXT), '
        'okg.finish_attribute_integrity_transition(TEXT, TEXT, TEXT), '
        'okg.abort_attribute_integrity_transition(TEXT, TEXT, TEXT), '
        'okg.register_attribute_integrity_scope(TEXT, TEXT, TEXT), '
        'okg.mark_attribute_integrity_scope_dirty(TEXT, TEXT, TEXT, TEXT), '
        'okg.finalize_attribute_integrity_receipt('
        'TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, BIGINT, TEXT, '
        'BIGINT, BIGINT, BIGINT, BIGINT, INTEGER, TEXT, TEXT, '
        'NUMERIC, NUMERIC, NUMERIC, NUMERIC, TEXT, BIGINT'
        ') FROM %I',
        role_name
      );
    END IF;
  END LOOP;
  FOREACH role_name IN ARRAY ARRAY['okg_substrate', 'app_rw'] LOOP
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role_name) THEN
      EXECUTE format(
        'GRANT EXECUTE ON FUNCTION '
        'okg.begin_attribute_integrity_transition(TEXT, TEXT, TEXT), '
        'okg.attribute_integrity_transition_matches(TEXT, TEXT, TEXT), '
        'okg.finish_attribute_integrity_transition(TEXT, TEXT, TEXT), '
        'okg.abort_attribute_integrity_transition(TEXT, TEXT, TEXT), '
        'okg.register_attribute_integrity_scope(TEXT, TEXT, TEXT), '
        'okg.mark_attribute_integrity_scope_dirty(TEXT, TEXT, TEXT, TEXT), '
        'okg.finalize_attribute_integrity_receipt('
        'TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, BIGINT, TEXT, '
        'BIGINT, BIGINT, BIGINT, BIGINT, INTEGER, TEXT, TEXT, '
        'NUMERIC, NUMERIC, NUMERIC, NUMERIC, TEXT, BIGINT'
        ') TO %I',
        role_name
      );
    END IF;
  END LOOP;
END
$privileges$;
