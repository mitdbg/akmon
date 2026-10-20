SET lock_timeout = '5s';
SET statement_timeout = '30s';

CREATE TABLE IF NOT EXISTS okg.effective_attribute_recovery_attempts (
  classification_binding_digest TEXT PRIMARY KEY,
  deployment_name TEXT NOT NULL,
  branch_name TEXT NOT NULL,
  predecessor_generation_id TEXT NOT NULL,
  expected_classification JSONB NOT NULL,
  stage TEXT NOT NULL CHECK (
    stage IN ('classified', 'snapshot_committed', 'interval_committed', 'prepared')
  ),
  authorization_payload JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
