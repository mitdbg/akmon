ALTER TABLE okg.compacted_generations
  ADD COLUMN IF NOT EXISTS target_generation_id BIGINT;

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
