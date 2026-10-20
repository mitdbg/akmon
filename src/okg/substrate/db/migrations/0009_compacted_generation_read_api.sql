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
      p_generation ~ '^[0-9]+$'
      AND target_generation_id = p_generation::bigint
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
