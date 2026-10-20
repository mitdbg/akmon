-- PACT harden-attribute-integrity-authority (PR #831 review follow-ups):
-- 1. Scope protected-write invalidation to the statement's actual rows via
--    transition tables (TRUNCATE stays global — it has no transition tables).
-- 2. Enforce receipt chain linkage server-side: an incremental receipt must
--    extend the scope's current chain head; an anchor (NULL prior) resets it.
-- 3. Narrow the transition/register/dirty/finalize APIs to okg_substrate;
--    app_rw keeps SELECT on the state/receipt tables only.
--
-- No mutation-epoch bump here: replacing these functions changes the schema
-- identity digest, so every existing receipt already fails the fast path and
-- forces one full re-anchor. Receipts minted before this migration were
-- honest; invalidating them twice adds nothing.

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
     AND mutation_epoch = p_mutation_epoch
     -- Incremental receipts must extend the scope's current chain
     -- head; an anchor (NULL prior) resets the chain.
     AND (p_prior_receipt_digest IS NULL
          OR latest_receipt_digest
             IS NOT DISTINCT FROM p_prior_receipt_digest);
  IF NOT FOUND THEN
    -- Distinguish a chain fork (epoch matched, prior did not)
    -- from the benign transition/epoch race for operability.
    IF EXISTS (
      SELECT 1 FROM okg.attribute_effective_integrity_state
       WHERE deployment_name = p_deployment
         AND branch_id = p_branch
         AND namespace = p_namespace
         AND mutation_epoch = p_mutation_epoch
    ) THEN
      RAISE EXCEPTION
        'attribute integrity receipt chain head moved during finalization';
    END IF;
    RAISE EXCEPTION
      'attribute integrity transition changed during finalization';
  END IF;
  RETURN TRUE;
END
$function$;

CREATE OR REPLACE FUNCTION okg.mark_attribute_effective_integrity_dirty()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $function$
BEGIN
  -- TRUNCATE has no transition tables, so its invalidation is
  -- necessarily global: every scope's epoch advances and every
  -- other backend's transition token dies.
  UPDATE okg.attribute_effective_integrity_state
     SET mutation_epoch = mutation_epoch + 1,
         dirty = TRUE,
         dirty_reason = TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME || ':' || TG_OP,
         updated_at = now();
  DELETE FROM okg.attribute_effective_integrity_transitions
   WHERE backend_pid <> pg_backend_pid()
      OR backend_start IS DISTINCT FROM (
        SELECT backend_start FROM pg_stat_activity
         WHERE pid = pg_backend_pid()
      );
  RETURN NULL;
END
$function$;

CREATE OR REPLACE FUNCTION okg.mark_attribute_effective_integrity_dirty_rows()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $function$
BEGIN
  -- Scoped invalidation from the statement's transition table
  -- (NEW TABLE for INSERT, OLD TABLE for DELETE; both are named
  -- changed_rows by the trigger). Only the scopes the statement
  -- actually touched lose their clean proof; the interval table
  -- carries the branch, the snapshot table dirties every branch
  -- of the touched namespaces. A zero-row statement dirties
  -- nothing, which is exact.
  IF TG_TABLE_NAME = 'attribute_effective_view_intervals' THEN
    UPDATE okg.attribute_effective_integrity_state s
       SET mutation_epoch = s.mutation_epoch + 1,
           dirty = TRUE,
           dirty_reason = TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME
             || ':' || TG_OP,
           updated_at = now()
     WHERE EXISTS (
       SELECT 1 FROM changed_rows c
        WHERE c.namespace = s.namespace AND c.branch_id = s.branch_id
     );
    DELETE FROM okg.attribute_effective_integrity_transitions t
     WHERE (t.backend_pid <> pg_backend_pid()
            OR t.backend_start IS DISTINCT FROM (
              SELECT backend_start FROM pg_stat_activity
               WHERE pid = pg_backend_pid()
            ))
       AND EXISTS (
         SELECT 1 FROM changed_rows c
          WHERE c.namespace = t.namespace
            AND c.branch_id = t.branch_id
       );
  ELSE
    UPDATE okg.attribute_effective_integrity_state s
       SET mutation_epoch = s.mutation_epoch + 1,
           dirty = TRUE,
           dirty_reason = TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME
             || ':' || TG_OP,
           updated_at = now()
     WHERE EXISTS (
       SELECT 1 FROM changed_rows c WHERE c.namespace = s.namespace
     );
    DELETE FROM okg.attribute_effective_integrity_transitions t
     WHERE (t.backend_pid <> pg_backend_pid()
            OR t.backend_start IS DISTINCT FROM (
              SELECT backend_start FROM pg_stat_activity
               WHERE pid = pg_backend_pid()
            ))
       AND EXISTS (
         SELECT 1 FROM changed_rows c WHERE c.namespace = t.namespace
       );
  END IF;
  RETURN NULL;
END
$function$;

CREATE OR REPLACE FUNCTION okg.mark_attribute_effective_integrity_dirty_update()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $function$
BEGIN
  -- UPDATE scope is the union of the old and new row coordinates:
  -- a row moved across namespaces or branches dirties both sides.
  IF TG_TABLE_NAME = 'attribute_effective_view_intervals' THEN
    UPDATE okg.attribute_effective_integrity_state s
       SET mutation_epoch = s.mutation_epoch + 1,
           dirty = TRUE,
           dirty_reason = TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME
             || ':' || TG_OP,
           updated_at = now()
     WHERE EXISTS (
         SELECT 1 FROM old_rows c
          WHERE c.namespace = s.namespace
            AND c.branch_id = s.branch_id
       )
        OR EXISTS (
         SELECT 1 FROM new_rows c
          WHERE c.namespace = s.namespace
            AND c.branch_id = s.branch_id
       );
    DELETE FROM okg.attribute_effective_integrity_transitions t
     WHERE (t.backend_pid <> pg_backend_pid()
            OR t.backend_start IS DISTINCT FROM (
              SELECT backend_start FROM pg_stat_activity
               WHERE pid = pg_backend_pid()
            ))
       AND (EXISTS (
             SELECT 1 FROM old_rows c
              WHERE c.namespace = t.namespace
                AND c.branch_id = t.branch_id
           )
            OR EXISTS (
             SELECT 1 FROM new_rows c
              WHERE c.namespace = t.namespace
                AND c.branch_id = t.branch_id
           ));
  ELSE
    UPDATE okg.attribute_effective_integrity_state s
       SET mutation_epoch = s.mutation_epoch + 1,
           dirty = TRUE,
           dirty_reason = TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME
             || ':' || TG_OP,
           updated_at = now()
     WHERE EXISTS (
         SELECT 1 FROM old_rows c WHERE c.namespace = s.namespace
       )
        OR EXISTS (
         SELECT 1 FROM new_rows c WHERE c.namespace = s.namespace
       );
    DELETE FROM okg.attribute_effective_integrity_transitions t
     WHERE (t.backend_pid <> pg_backend_pid()
            OR t.backend_start IS DISTINCT FROM (
              SELECT backend_start FROM pg_stat_activity
               WHERE pid = pg_backend_pid()
            ))
       AND (EXISTS (
             SELECT 1 FROM old_rows c
              WHERE c.namespace = t.namespace
           )
            OR EXISTS (
             SELECT 1 FROM new_rows c
              WHERE c.namespace = t.namespace
           ));
  END IF;
  RETURN NULL;
END
$function$;

DO $triggers$
DECLARE
  protected_table regclass;
  trigger_name text;
  legacy_name text;
  operation text;
BEGIN
  FOREACH protected_table IN ARRAY ARRAY[
    'okg.attribute_effective_view'::regclass,
    'okg.attribute_effective_view_intervals'::regclass
  ] LOOP
    FOREACH operation IN ARRAY ARRAY['INSERT', 'UPDATE', 'DELETE'] LOOP
      legacy_name := 'attribute_integrity_dirty_' || lower(operation) ||
        '_' || substr(md5(protected_table::text), 1, 10);
      EXECUTE format(
        'DROP TRIGGER IF EXISTS %I ON %s',
        legacy_name, protected_table
      );
      trigger_name := 'attribute_integrity_scoped_' || lower(operation)
        || '_' || substr(md5(protected_table::text), 1, 10);
      IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
         WHERE tgrelid = protected_table AND tgname = trigger_name
      ) THEN
        IF operation = 'INSERT' THEN
          EXECUTE format(
            'CREATE TRIGGER %I AFTER INSERT ON %s '
            'REFERENCING NEW TABLE AS changed_rows '
            'FOR EACH STATEMENT EXECUTE FUNCTION '
            'okg.mark_attribute_effective_integrity_dirty_rows()',
            trigger_name, protected_table
          );
        ELSIF operation = 'DELETE' THEN
          EXECUTE format(
            'CREATE TRIGGER %I AFTER DELETE ON %s '
            'REFERENCING OLD TABLE AS changed_rows '
            'FOR EACH STATEMENT EXECUTE FUNCTION '
            'okg.mark_attribute_effective_integrity_dirty_rows()',
            trigger_name, protected_table
          );
        ELSE
          EXECUTE format(
            'CREATE TRIGGER %I AFTER UPDATE ON %s '
            'REFERENCING OLD TABLE AS old_rows '
            'NEW TABLE AS new_rows '
            'FOR EACH STATEMENT EXECUTE FUNCTION '
            'okg.mark_attribute_effective_integrity_dirty_update()',
            trigger_name, protected_table
          );
        END IF;
      END IF;
    END LOOP;
    trigger_name := 'attribute_integrity_dirty_truncate_' ||
      substr(md5(protected_table::text), 1, 10);
    IF NOT EXISTS (
      SELECT 1 FROM pg_trigger
       WHERE tgrelid = protected_table AND tgname = trigger_name
    ) THEN
      EXECUTE format(
        'CREATE TRIGGER %I AFTER TRUNCATE ON %s FOR EACH STATEMENT '
        'EXECUTE FUNCTION okg.mark_attribute_effective_integrity_dirty()',
        trigger_name, protected_table
      );
    END IF;
  END LOOP;
END
$triggers$;

DO $privileges$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_rw') THEN
    -- The publisher runs as okg_substrate; app_rw keeps SELECT on the
    -- state/receipt tables but loses every transition/finalizer API.
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
      FROM app_rw;
  END IF;
END
$privileges$;
