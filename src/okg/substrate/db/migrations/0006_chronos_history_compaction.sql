CREATE TABLE IF NOT EXISTS okg.history_compaction_plans (
  plan_id UUID PRIMARY KEY,
  deployment_name TEXT NOT NULL,
  branch_id TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('planned', 'applying', 'complete', 'failed')),
  plan JSONB NOT NULL,
  progress JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS okg.compacted_generations (
  checkpoint_id TEXT PRIMARY KEY,
  deployment_name TEXT NOT NULL,
  branch_id TEXT NOT NULL,
  plan_id UUID NOT NULL REFERENCES okg.history_compaction_plans(plan_id),
  compacted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  reason TEXT NOT NULL DEFAULT 'history_retention'
);

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
