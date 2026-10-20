SET lock_timeout = '5s';
SET statement_timeout = '30s';

ALTER TABLE okg.effective_attribute_recovery_attempts
  ADD COLUMN IF NOT EXISTS consumed_by_generation_id TEXT,
  ADD COLUMN IF NOT EXISTS consumed_at TIMESTAMPTZ;
