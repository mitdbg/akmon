-- Durable per-item/signature retry budget for model-backed enrichers.
-- Empty on upgrade: legacy held progress remains held and starts a fresh
-- classified item budget on its next run.
CREATE TABLE IF NOT EXISTS okg.enricher_input_quarantines (
  deployment_name    TEXT        NOT NULL,
  branch_name        TEXT        NOT NULL,
  enricher_name      TEXT        NOT NULL,
  item_ref           TEXT        NOT NULL,
  input_signature    TEXT        NOT NULL,
  operator_version   TEXT        NOT NULL,
  model_id           TEXT        NOT NULL,
  prompt_hash        TEXT        NOT NULL,
  input_hash         TEXT        NOT NULL,
  status             TEXT        NOT NULL,
  attempt_count      INTEGER     NOT NULL,
  attempt_limit      INTEGER     NOT NULL,
  last_failure_class TEXT,
  last_error         TEXT,
  first_failed_at    TIMESTAMPTZ,
  last_failed_at     TIMESTAMPTZ,
  quarantined_at     TIMESTAMPTZ,
  resolved_at        TIMESTAMPTZ,
  superseded_at      TIMESTAMPTZ,
  claim_token        UUID,
  claimed_at         TIMESTAMPTZ,
  last_generation_id TEXT,
  created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (
    deployment_name, branch_name, enricher_name, item_ref, input_signature
  ),
  CONSTRAINT enricher_input_quarantine_status_chk
    CHECK (status IN ('retrying', 'active', 'superseded', 'resolved')),
  CONSTRAINT enricher_input_quarantine_attempt_count_chk
    CHECK (attempt_count >= 0 AND attempt_count <= attempt_limit),
  CONSTRAINT enricher_input_quarantine_attempt_limit_chk
    CHECK (attempt_limit > 0),
  CONSTRAINT enricher_input_quarantine_active_chk
    CHECK (
      status <> 'active'
      OR (attempt_count = attempt_limit AND quarantined_at IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS enricher_input_quarantine_active_idx
  ON okg.enricher_input_quarantines
    (deployment_name, status, branch_name, enricher_name, item_ref)
  WHERE status = 'active';

GRANT SELECT, INSERT, UPDATE, DELETE ON
  okg.enricher_input_quarantines
TO app_rw;
