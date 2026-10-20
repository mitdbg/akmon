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
