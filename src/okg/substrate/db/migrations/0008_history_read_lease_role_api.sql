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
  IF EXISTS (
       SELECT 1 FROM okg.compacted_generations
        WHERE checkpoint_id = p_checkpoint_id
     )
     OR NOT EXISTS (
       SELECT 1 FROM public._chronos_branch_interval_checkpoints
        WHERE checkpoint_id = p_checkpoint_id
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
