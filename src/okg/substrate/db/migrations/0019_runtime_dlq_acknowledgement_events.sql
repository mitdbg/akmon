-- PACT acknowledge-runtime-dlq:
-- preserve DBOS terminal history while allowing exact, witnessed dispositions.

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE IF NOT EXISTS okg.runtime_dlq_acknowledgement_events (
  acknowledgement_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  deployment_name          TEXT NOT NULL CHECK (btrim(deployment_name) <> ''),
  workflow_id              TEXT NOT NULL CHECK (btrim(workflow_id) <> ''),
  workflow_name            TEXT NOT NULL CHECK (btrim(workflow_name) <> ''),
  application_version      TEXT NOT NULL CHECK (btrim(application_version) <> ''),
  failure_snapshot_digest  TEXT NOT NULL
    CHECK (failure_snapshot_digest ~ '^sha256:[0-9a-f]{64}$'),
  error_digest             TEXT NOT NULL
    CHECK (error_digest ~ '^sha256:[0-9a-f]{64}$'),
  responsibility           JSONB NOT NULL,
  actor                    TEXT NOT NULL CHECK (btrim(actor) <> ''),
  reason                   TEXT NOT NULL CHECK (btrim(reason) <> ''),
  candidate_revision       TEXT NOT NULL CHECK (btrim(candidate_revision) <> ''),
  operation_receipt_digest TEXT NOT NULL
    CHECK (operation_receipt_digest ~ '^sha256:[0-9a-f]{64}$'),
  witness                  JSONB NOT NULL,
  plan_payload             JSONB NOT NULL,
  plan_id                  TEXT NOT NULL
    CHECK (plan_id ~ '^sha256:[0-9a-f]{64}$'),
  acknowledged_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (workflow_id)
);

CREATE INDEX IF NOT EXISTS runtime_dlq_ack_events_created_idx
  ON okg.runtime_dlq_acknowledgement_events
  (deployment_name, acknowledged_at DESC);

CREATE OR REPLACE FUNCTION okg.reject_runtime_dlq_acknowledgement_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
BEGIN
  RAISE EXCEPTION 'runtime DLQ acknowledgement events are append-only';
END
$function$;

DROP TRIGGER IF EXISTS runtime_dlq_acknowledgements_immutable
  ON okg.runtime_dlq_acknowledgement_events;
CREATE TRIGGER runtime_dlq_acknowledgements_immutable
  BEFORE UPDATE OR DELETE OR TRUNCATE
  ON okg.runtime_dlq_acknowledgement_events
  FOR EACH STATEMENT
  EXECUTE FUNCTION okg.reject_runtime_dlq_acknowledgement_mutation();

DO $privileges$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_rw') THEN
    REVOKE INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON
      okg.runtime_dlq_acknowledgement_events FROM app_rw;
    GRANT SELECT ON okg.runtime_dlq_acknowledgement_events TO app_rw;
  END IF;
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'catalog_admin') THEN
    REVOKE INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON
      okg.runtime_dlq_acknowledgement_events FROM catalog_admin;
    GRANT SELECT ON okg.runtime_dlq_acknowledgement_events TO catalog_admin;
  END IF;
END
$privileges$;
