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
    OR target_generation_id = CASE
      WHEN p_generation ~ '^[0-9]+$' THEN p_generation::bigint
      ELSE NULL
    END
  )
  AND (p_deployment_name IS NULL OR deployment_name = p_deployment_name)
  ORDER BY compacted_at DESC
  LIMIT 1
$$;
