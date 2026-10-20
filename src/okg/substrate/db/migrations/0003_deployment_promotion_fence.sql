SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE IF NOT EXISTS okg.deployment_promotion_fences (
  deployment_name       TEXT PRIMARY KEY,
  promotion_id          TEXT NOT NULL,
  status                TEXT NOT NULL DEFAULT 'fenced'
    CHECK (status IN ('fenced', 'recovery_blocked')),
  schedules_removed     INTEGER NOT NULL DEFAULT 0
    CHECK (schedules_removed >= 0),
  schedule_snapshot     JSONB NOT NULL DEFAULT '[]'::jsonb,
  fenced_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
  blocker               JSONB,
  metadata              JSONB NOT NULL DEFAULT '{}'::jsonb
);
