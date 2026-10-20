-- Persist typed evidence for successful participant DBOS workflows.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE IF NOT EXISTS okg.workflow_operational_registrations (
  workflow_uuid    TEXT PRIMARY KEY,
  deployment_name  TEXT NOT NULL CHECK (btrim(deployment_name) <> ''),
  workflow_name    TEXT NOT NULL CHECK (btrim(workflow_name) <> ''),
  queue_name       TEXT NOT NULL CHECK (btrim(queue_name) <> ''),
  contract_version SMALLINT NOT NULL
    CONSTRAINT workflow_operational_registrations_version_check
    CHECK (contract_version = 1),
  registered_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS okg.workflow_operational_outcomes (
  workflow_uuid    TEXT PRIMARY KEY
    REFERENCES okg.workflow_operational_registrations(workflow_uuid),
  contract_version SMALLINT NOT NULL
    CONSTRAINT workflow_operational_outcomes_version_check
    CHECK (contract_version = 1),
  outcome          TEXT NOT NULL
    CONSTRAINT workflow_operational_outcomes_outcome_check
    CHECK (outcome IN ('clean', 'partial_failure', 'retained_failure', 'skipped')),
  succeeded_count  INTEGER NOT NULL CHECK (succeeded_count >= 0),
  failed_count     INTEGER NOT NULL CHECK (failed_count >= 0),
  semantic_units   JSONB NOT NULL CHECK (jsonb_typeof(semantic_units) = 'object'),
  details          JSONB NOT NULL DEFAULT '{}'::jsonb
    CHECK (jsonb_typeof(details) = 'object'),
  recorded_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT workflow_operational_outcomes_counts_check CHECK (
    (outcome = 'clean' AND failed_count = 0)
    OR (outcome = 'partial_failure' AND succeeded_count > 0 AND failed_count > 0)
    OR (outcome = 'retained_failure' AND failed_count > 0)
    OR (outcome = 'skipped' AND succeeded_count = 0 AND failed_count = 0)
  )
);

CREATE INDEX IF NOT EXISTS workflow_operational_registrations_binding_idx
  ON okg.workflow_operational_registrations
    (deployment_name, workflow_name, queue_name, registered_at DESC);
CREATE INDEX IF NOT EXISTS workflow_operational_outcomes_recorded_idx
  ON okg.workflow_operational_outcomes (recorded_at DESC);

DO $privileges$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_rw') THEN
    REVOKE UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON
      okg.workflow_operational_registrations,
      okg.workflow_operational_outcomes FROM app_rw;
    GRANT SELECT, INSERT ON
      okg.workflow_operational_registrations,
      okg.workflow_operational_outcomes TO app_rw;
  END IF;
END
$privileges$;
