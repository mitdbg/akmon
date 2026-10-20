SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE okg.deployment_promotion_fences
  DROP CONSTRAINT IF EXISTS deployment_promotion_fences_status_check;

ALTER TABLE okg.deployment_promotion_fences
  ADD CONSTRAINT deployment_promotion_fences_status_check
  CHECK (status IN ('fenced', 'recovery_blocked', 'released'));
