-- External action review ledger.  This migration is intentionally additive and
-- idempotent: schema.sql contains the same current-state definitions for fresh
-- installs, while this file upgrades deployments that have already applied the
-- preceding migration ordinals.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

-- Existing installs used the generic ``deployment`` identity column. Rename
-- it before replaying any canonical table DDL: CREATE TABLE IF NOT EXISTS does
-- not repair an existing table, and later foreign keys already name the
-- canonical column. A mixed state is refused so the migration never leaves
-- two competing identity columns behind.
DO $external_action_deployment_identity_cutover$
DECLARE
  relation_name TEXT;
  has_legacy_column BOOLEAN;
  has_canonical_column BOOLEAN;
BEGIN
  FOREACH relation_name IN ARRAY ARRAY[
    'external_action_proposals',
    'external_action_payload_versions',
    'external_action_auth_audits',
    'external_action_dpop_replay_nonces',
    'external_action_decisions',
    'external_action_transition_idempotency',
    'external_action_executions',
    'external_action_owner_unavailability_attestations',
    'external_action_outcomes',
    'external_action_events',
    'external_action_execution_epoch_leases',
    'external_action_restore_attestations',
    'external_action_restore_fences',
    'external_action_restore_sessions',
    'external_action_restore_completion_receipts',
    'external_action_restore_finalize_idempotency',
    'external_action_restore_checkpoint_staging',
    'external_action_restore_checkpoint_entries',
    'external_action_restore_recoveries',
    'external_action_api_role_bindings'
  ] LOOP
    SELECT EXISTS (
      SELECT 1
        FROM pg_catalog.pg_attribute a
        JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
       WHERE n.nspname = 'okg'
         AND c.relname = relation_name
         AND c.relkind = 'r'
         AND a.attname = 'deployment'
         AND NOT a.attisdropped
    ), EXISTS (
      SELECT 1
        FROM pg_catalog.pg_attribute a
        JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
       WHERE n.nspname = 'okg'
         AND c.relname = relation_name
         AND c.relkind = 'r'
         AND a.attname = 'deployment_name'
         AND NOT a.attisdropped
    )
      INTO has_legacy_column, has_canonical_column;
    IF has_legacy_column AND has_canonical_column THEN
      RAISE EXCEPTION 'external action relation % has both deployment identity columns', relation_name
        USING ERRCODE = '42701';
    ELSIF has_legacy_column THEN
      EXECUTE format(
        'ALTER TABLE okg.%I RENAME COLUMN %I TO %I',
        relation_name, 'deployment', 'deployment_name'
      );
    END IF;
  END LOOP;
END
$external_action_deployment_identity_cutover$;

CREATE TABLE IF NOT EXISTS okg.external_action_proposals (
  proposal_id TEXT PRIMARY KEY,
  deployment_name TEXT NOT NULL CHECK (btrim(deployment_name) <> ''),
  execution_epoch BIGINT NOT NULL CHECK (execution_epoch >= 0),
  effect_instance_key TEXT NOT NULL CHECK (btrim(effect_instance_key) <> ''),
  submission_content_hash TEXT NOT NULL,
  submission_request_hash TEXT NOT NULL,
  submission_idempotency_key_hash TEXT NOT NULL,
  proposer_separation_subject TEXT NOT NULL,
  automation_id TEXT NOT NULL, automation_version TEXT NOT NULL,
  run_id TEXT NOT NULL, correlation_id TEXT NOT NULL,
  action_type TEXT NOT NULL, target_ref TEXT NOT NULL, risk_tier TEXT NOT NULL,
  action_schema_digest TEXT NOT NULL, action_contract_digest TEXT NOT NULL,
  executor_contract_digest TEXT NOT NULL, payload_renderer_digest TEXT NOT NULL,
  governed_tool_binding_digest TEXT NOT NULL,
  provider_connection_contract_digest TEXT NOT NULL,
  provider_capability_digest TEXT NOT NULL, provider_operation_id TEXT NOT NULL,
  provider_operation_descriptor_digest TEXT NOT NULL,
  original_payload_hash TEXT NOT NULL,
  requested_review_expires_at TIMESTAMPTZ NOT NULL,
  effective_review_expires_at TIMESTAMPTZ NOT NULL,
  summary_code TEXT NOT NULL,
  evidence_digests JSONB NOT NULL DEFAULT '[]'::jsonb,
  review_state TEXT NOT NULL DEFAULT 'pending'
    CHECK (review_state IN ('pending','edited','approved','rejected','expired','cancelled')),
  execution_state TEXT NOT NULL DEFAULT 'unavailable'
    CHECK (execution_state IN ('unavailable','ready','claimed','succeeded','failed','indeterminate')),
  lifecycle_version BIGINT NOT NULL DEFAULT 1 CHECK (lifecycle_version > 0),
  payload_version BIGINT NOT NULL DEFAULT 1 CHECK (payload_version > 0),
  current_payload_hash TEXT NOT NULL,
  current_decision_id TEXT, current_decision_binding_hash TEXT,
  execute_before TIMESTAMPTZ, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), expired_at TIMESTAMPTZ,
  UNIQUE (deployment_name, proposal_id), UNIQUE (deployment_name, effect_instance_key),
  UNIQUE (deployment_name, proposer_separation_subject, submission_idempotency_key_hash)
);

CREATE TABLE IF NOT EXISTS okg.external_action_payload_versions (
  payload_version_id BIGSERIAL PRIMARY KEY, deployment_name TEXT NOT NULL,
  proposal_id TEXT NOT NULL, payload_version BIGINT NOT NULL,
  payload_hash TEXT NOT NULL, parent_payload_hash TEXT,
  canonical_payload BYTEA NOT NULL,
  difference_field_codes JSONB NOT NULL DEFAULT '[]'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (deployment_name, proposal_id, payload_version),
  UNIQUE (deployment_name, proposal_id, payload_hash),
  FOREIGN KEY (deployment_name, proposal_id)
    REFERENCES okg.external_action_proposals(deployment_name, proposal_id)
    ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS okg.external_action_auth_audits (
  authentication_audit_id TEXT PRIMARY KEY,
  deployment_name TEXT NOT NULL CHECK (btrim(deployment_name) <> ''),
  request_id TEXT NOT NULL, principal TEXT NOT NULL,
  separation_subject TEXT NOT NULL, nomos_provider_id TEXT NOT NULL,
  nomos_subject_binding_id TEXT NOT NULL, authentication_method_code TEXT NOT NULL,
  issuer_or_trust_domain_digest TEXT NOT NULL,
  sender_key_or_certificate_thumbprint TEXT, token_or_certificate_id_digest TEXT,
  dpop_jti_digest TEXT, auth_config_digest TEXT NOT NULL,
  result_code TEXT NOT NULL, observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  credential_expires_at TIMESTAMPTZ,
  provider_status TEXT, binding_status TEXT,
  provider_updated_at TIMESTAMPTZ, binding_updated_at TIMESTAMPTZ,
  provider_credential_generation_digest TEXT,
  binding_credential_generation_digest TEXT,
  credential_generation_digest TEXT,
  presented_credential_generation_digest TEXT,
  token_issued_at TIMESTAMPTZ,
  certificate_thumbprint TEXT,
  certificate_not_before TIMESTAMPTZ,
  certificate_not_after TIMESTAMPTZ,
  verifier_assertion_nonce_digest TEXT,
  verifier_assertion_expires_at TIMESTAMPTZ,
  verifier_transition_digest TEXT,
  verifier_policy_input_digest TEXT,
  verifier_required_obligations_digest TEXT,
  verifier_completed_obligations_digest TEXT,
  verifier_missing_obligations_digest TEXT,
  verifier_assertion_tag TEXT,
  verifier_capability_generation BIGINT,
  CONSTRAINT external_action_auth_audits_provider_status_check
    CHECK (provider_status IS NULL OR provider_status IN ('active','disabled','rotating','revoked')),
  CONSTRAINT external_action_auth_audits_binding_status_check
    CHECK (binding_status IS NULL OR binding_status IN ('active','disabled')),
  CONSTRAINT external_action_auth_audits_provider_generation_check
    CHECK (provider_credential_generation_digest IS NULL OR provider_credential_generation_digest ~ '^sha256:[0-9a-f]{64}$'),
  CONSTRAINT external_action_auth_audits_binding_generation_check
    CHECK (binding_credential_generation_digest IS NULL OR binding_credential_generation_digest ~ '^sha256:[0-9a-f]{64}$'),
  CONSTRAINT external_action_auth_audits_credential_generation_check
    CHECK (credential_generation_digest IS NULL OR credential_generation_digest ~ '^sha256:[0-9a-f]{64}$'),
  CONSTRAINT external_action_auth_audits_presented_generation_check
    CHECK (presented_credential_generation_digest IS NULL OR presented_credential_generation_digest ~ '^sha256:[0-9a-f]{64}$'),
  CONSTRAINT external_action_auth_audits_certificate_thumbprint_check
    CHECK (certificate_thumbprint IS NULL OR (btrim(certificate_thumbprint) = certificate_thumbprint AND octet_length(certificate_thumbprint) BETWEEN 1 AND 512)),
  CONSTRAINT external_action_auth_audits_certificate_interval_check
    CHECK (certificate_not_before IS NULL OR certificate_not_after IS NULL OR certificate_not_after >= certificate_not_before),
  nomos_decision_id TEXT,
  UNIQUE (deployment_name, authentication_audit_id),
  UNIQUE (deployment_name, verifier_capability_generation, verifier_assertion_nonce_digest),
  FOREIGN KEY (deployment_name, nomos_decision_id)
    REFERENCES okg.nomos_decisions(deployment_name, decision_id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS okg.external_action_dpop_replay_nonces (
  dpop_replay_nonce_id BIGSERIAL PRIMARY KEY, deployment_name TEXT NOT NULL,
  issuer_digest TEXT NOT NULL, sender_key_thumbprint TEXT NOT NULL,
  jti_digest TEXT NOT NULL, request_method TEXT NOT NULL,
  normalized_request_uri_digest TEXT NOT NULL, access_token_hash TEXT NOT NULL,
  observed_at TIMESTAMPTZ NOT NULL DEFAULT now(), expires_at TIMESTAMPTZ NOT NULL,
  UNIQUE (deployment_name, issuer_digest, sender_key_thumbprint, jti_digest)
);

CREATE TABLE IF NOT EXISTS okg.external_action_decisions (
  decision_row_id BIGSERIAL PRIMARY KEY, decision_id TEXT NOT NULL,
  deployment_name TEXT NOT NULL, proposal_id TEXT NOT NULL,
  verdict TEXT NOT NULL CHECK (verdict IN ('approved','rejected')),
  expected_lifecycle_version BIGINT NOT NULL, expected_payload_version BIGINT NOT NULL,
  approved_lifecycle_version BIGINT NOT NULL, approved_payload_version BIGINT NOT NULL,
  approved_payload_hash TEXT NOT NULL, action_type TEXT NOT NULL,
  target_ref TEXT NOT NULL, action_schema_digest TEXT NOT NULL,
  action_contract_digest TEXT NOT NULL, executor_contract_digest TEXT NOT NULL,
  payload_renderer_digest TEXT NOT NULL, governed_tool_binding_digest TEXT NOT NULL,
  provider_connection_contract_digest TEXT NOT NULL, provider_capability_digest TEXT NOT NULL,
  provider_operation_id TEXT NOT NULL, provider_operation_descriptor_digest TEXT NOT NULL,
  execution_epoch BIGINT NOT NULL, effect_instance_key TEXT NOT NULL,
  approver_principal TEXT NOT NULL, approver_separation_subject TEXT NOT NULL,
  authentication_audit_id TEXT NOT NULL, nomos_decision_id TEXT NOT NULL,
  policy_digest TEXT NOT NULL, execute_before TIMESTAMPTZ, reason_code TEXT,
  decision_binding_hash TEXT NOT NULL, request_hash TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(), UNIQUE (deployment_name, decision_id),
  FOREIGN KEY (deployment_name, proposal_id)
    REFERENCES okg.external_action_proposals(deployment_name, proposal_id) ON DELETE RESTRICT,
  FOREIGN KEY (deployment_name, authentication_audit_id)
    REFERENCES okg.external_action_auth_audits(deployment_name, authentication_audit_id) ON DELETE RESTRICT,
  FOREIGN KEY (deployment_name, nomos_decision_id)
    REFERENCES okg.nomos_decisions(deployment_name, decision_id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS okg.external_action_transition_idempotency (
  transition_idempotency_id BIGSERIAL PRIMARY KEY, deployment_name TEXT NOT NULL,
  proposal_id TEXT NOT NULL,
  operation TEXT NOT NULL CHECK (operation IN ('edit','defer','approve','approve_with_edit','reject','cancel')),
  actor_separation_subject TEXT NOT NULL, idempotency_key_hash TEXT NOT NULL,
  transition_request_hash TEXT NOT NULL, response_status INTEGER NOT NULL,
  response_media_type TEXT NOT NULL, response_body_jcs BYTEA NOT NULL,
  result_event_ids JSONB NOT NULL DEFAULT '[]'::jsonb, result_payload_version BIGINT,
  result_decision_id TEXT, result_lifecycle_version BIGINT, result_etag TEXT,
  committed_at TIMESTAMPTZ NOT NULL DEFAULT now(), authentication_audit_id TEXT NOT NULL,
  nomos_decision_id TEXT NOT NULL,
  UNIQUE (deployment_name, proposal_id, actor_separation_subject, idempotency_key_hash),
  FOREIGN KEY (deployment_name, proposal_id)
    REFERENCES okg.external_action_proposals(deployment_name, proposal_id) ON DELETE RESTRICT,
  FOREIGN KEY (deployment_name, authentication_audit_id)
    REFERENCES okg.external_action_auth_audits(deployment_name, authentication_audit_id) ON DELETE RESTRICT,
  FOREIGN KEY (deployment_name, nomos_decision_id)
    REFERENCES okg.nomos_decisions(deployment_name, decision_id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS okg.external_action_executions (
  execution_id TEXT PRIMARY KEY, deployment_name TEXT NOT NULL, proposal_id TEXT NOT NULL,
  decision_id TEXT NOT NULL, execution_epoch BIGINT NOT NULL,
  execution_epoch_lease_digest TEXT NOT NULL, effect_instance_key TEXT NOT NULL,
  worker_separation_subject TEXT NOT NULL, approved_lifecycle_version BIGINT NOT NULL,
  approved_payload_version BIGINT NOT NULL, approved_payload_hash TEXT NOT NULL,
  action_type TEXT NOT NULL, target_ref TEXT NOT NULL, action_schema_digest TEXT NOT NULL,
  action_contract_digest TEXT NOT NULL, executor_contract_digest TEXT NOT NULL,
  payload_renderer_digest TEXT NOT NULL, governed_tool_binding_digest TEXT NOT NULL,
  provider_connection_contract_digest TEXT NOT NULL, provider_capability_digest TEXT NOT NULL,
  provider_operation_id TEXT NOT NULL, provider_operation_descriptor_digest TEXT NOT NULL,
  policy_digest TEXT NOT NULL, execution_binding_hash TEXT NOT NULL,
  request_hash TEXT NOT NULL, okg_claim_handoff_receipt_digest TEXT NOT NULL,
  handoff_protected_b64 TEXT NOT NULL, handoff_payload_b64 TEXT NOT NULL,
  handoff_signature_b64 TEXT NOT NULL, canonical_payload BYTEA NOT NULL,
  provider_idempotency_key_digest TEXT NOT NULL, authentication_audit_id TEXT NOT NULL,
  nomos_decision_id TEXT NOT NULL, claimed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  execute_before TIMESTAMPTZ NOT NULL, UNIQUE (deployment_name, proposal_id),
  FOREIGN KEY (deployment_name, proposal_id)
    REFERENCES okg.external_action_proposals(deployment_name, proposal_id) ON DELETE RESTRICT,
  FOREIGN KEY (deployment_name, decision_id)
    REFERENCES okg.external_action_decisions(deployment_name, decision_id) ON DELETE RESTRICT,
  FOREIGN KEY (deployment_name, authentication_audit_id)
    REFERENCES okg.external_action_auth_audits(deployment_name, authentication_audit_id) ON DELETE RESTRICT,
  FOREIGN KEY (deployment_name, nomos_decision_id)
    REFERENCES okg.nomos_decisions(deployment_name, decision_id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS okg.external_action_owner_unavailability_attestations (
  attestation_id BIGSERIAL PRIMARY KEY, deployment_name TEXT NOT NULL, execution_id TEXT NOT NULL,
  claim_owner_separation_subject TEXT NOT NULL, reconcile_not_before TIMESTAMPTZ NOT NULL,
  issued_at TIMESTAMPTZ NOT NULL, expires_at TIMESTAMPTZ NOT NULL, nonce TEXT NOT NULL,
  signer_separation_subject TEXT NOT NULL, authenticated_submitter_separation_subject TEXT NOT NULL,
  idempotency_key_hash TEXT NOT NULL, request_hash TEXT NOT NULL, response_status INTEGER NOT NULL,
  response_body_jcs BYTEA NOT NULL, protected_b64 TEXT NOT NULL, payload_b64 TEXT NOT NULL,
  signature_b64 TEXT NOT NULL, signer_key_id_digest TEXT NOT NULL, envelope_digest TEXT NOT NULL,
  verified_at TIMESTAMPTZ NOT NULL DEFAULT now(), authentication_audit_id TEXT NOT NULL,
  nomos_decision_id TEXT NOT NULL,
  UNIQUE (deployment_name, signer_separation_subject, nonce), UNIQUE (deployment_name, envelope_digest),
  UNIQUE (deployment_name, execution_id, envelope_digest),
  FOREIGN KEY (execution_id) REFERENCES okg.external_action_executions(execution_id) ON DELETE RESTRICT,
  FOREIGN KEY (deployment_name, authentication_audit_id)
    REFERENCES okg.external_action_auth_audits(deployment_name, authentication_audit_id) ON DELETE RESTRICT,
  FOREIGN KEY (deployment_name, nomos_decision_id)
    REFERENCES okg.nomos_decisions(deployment_name, decision_id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS okg.external_action_outcomes (
  outcome_id BIGSERIAL PRIMARY KEY, deployment_name TEXT NOT NULL, execution_id TEXT NOT NULL,
  effect_instance_key TEXT NOT NULL, worker_or_reconciler_separation_subject TEXT NOT NULL,
  outcome TEXT NOT NULL CHECK (outcome IN ('succeeded','failed','indeterminate')),
  outcome_reason_code TEXT NOT NULL, provider_receipt_digest TEXT,
  recovery_journal_digest TEXT NOT NULL, owner_unavailability_attestation_digest TEXT,
  request_hash TEXT NOT NULL, authentication_audit_id TEXT NOT NULL,
  nomos_decision_id TEXT NOT NULL, recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (deployment_name, execution_id),
  FOREIGN KEY (execution_id) REFERENCES okg.external_action_executions(execution_id) ON DELETE RESTRICT,
  FOREIGN KEY (deployment_name, execution_id, owner_unavailability_attestation_digest)
    REFERENCES okg.external_action_owner_unavailability_attestations(deployment_name, execution_id, envelope_digest) ON DELETE RESTRICT,
  FOREIGN KEY (deployment_name, authentication_audit_id)
    REFERENCES okg.external_action_auth_audits(deployment_name, authentication_audit_id) ON DELETE RESTRICT,
  FOREIGN KEY (deployment_name, nomos_decision_id)
    REFERENCES okg.nomos_decisions(deployment_name, decision_id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS okg.external_action_events (
  event_id BIGSERIAL PRIMARY KEY, deployment_name TEXT NOT NULL, proposal_id TEXT,
  execution_epoch BIGINT NOT NULL, effect_instance_key TEXT NOT NULL,
  sequence BIGINT NOT NULL, lifecycle_version BIGINT NOT NULL,
  event_type TEXT NOT NULL, actor_kind TEXT NOT NULL,
  actor_principal TEXT, actor_separation_subject TEXT,
  authentication_audit_ref TEXT, nomos_decision_ref TEXT, deadline_kind TEXT,
  policy_digest TEXT, before_review_state TEXT, after_review_state TEXT,
  before_execution_state TEXT, after_execution_state TEXT, payload_version BIGINT,
  payload_hash TEXT, decision_id TEXT, decision_binding_hash TEXT,
  execution_id TEXT, execution_binding_hash TEXT, transition_reason_code TEXT,
  outcome TEXT, outcome_reason_code TEXT, outcome_request_hash TEXT,
  restore_recovery_digest TEXT, effective_at TIMESTAMPTZ,
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(), previous_event_hash TEXT,
  event_hash TEXT NOT NULL, UNIQUE (deployment_name, effect_instance_key, sequence),
  UNIQUE (deployment_name, effect_instance_key, event_hash),
  FOREIGN KEY (deployment_name, proposal_id)
    REFERENCES okg.external_action_proposals(deployment_name, proposal_id) ON DELETE RESTRICT,
  FOREIGN KEY (deployment_name, authentication_audit_ref)
    REFERENCES okg.external_action_auth_audits(deployment_name, authentication_audit_id) ON DELETE RESTRICT,
  FOREIGN KEY (deployment_name, nomos_decision_ref)
    REFERENCES okg.nomos_decisions(deployment_name, decision_id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS okg.external_action_execution_epoch_leases (
  lease_row_id BIGSERIAL PRIMARY KEY, deployment_name TEXT NOT NULL, epoch BIGINT NOT NULL,
  lease_sequence BIGINT NOT NULL, previous_lease_digest TEXT, previous_epoch_digest TEXT,
  issued_at TIMESTAMPTZ NOT NULL, expires_at TIMESTAMPTZ NOT NULL,
  signer_separation_subject TEXT NOT NULL, nonce TEXT NOT NULL,
  protected_b64 TEXT NOT NULL, payload_b64 TEXT NOT NULL, signature_b64 TEXT NOT NULL,
  signer_key_id_digest TEXT NOT NULL, envelope_digest TEXT NOT NULL,
  verified_at TIMESTAMPTZ NOT NULL DEFAULT now(), verification_source TEXT NOT NULL,
  verification_config_digest TEXT NOT NULL, verification_login_binding_digest TEXT NOT NULL,
  lease_chain_digest TEXT, authentication_audit_id TEXT, nomos_decision_id TEXT,
  UNIQUE (deployment_name, epoch, lease_sequence),
  UNIQUE (deployment_name, signer_separation_subject, nonce), UNIQUE (deployment_name, envelope_digest),
  FOREIGN KEY (deployment_name, authentication_audit_id)
    REFERENCES okg.external_action_auth_audits(deployment_name, authentication_audit_id) ON DELETE RESTRICT,
  FOREIGN KEY (deployment_name, nomos_decision_id)
    REFERENCES okg.nomos_decisions(deployment_name, decision_id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS okg.external_action_restore_attestations (
  attestation_row_id BIGSERIAL PRIMARY KEY, deployment_name TEXT NOT NULL, restore_id TEXT NOT NULL,
  old_epoch BIGINT NOT NULL, new_epoch BIGINT NOT NULL, old_epoch_lease_digest TEXT NOT NULL,
  new_epoch_lease_digest TEXT NOT NULL, ledger_fence_envelope_digest TEXT NOT NULL,
  fenced_ledger_root_digest TEXT NOT NULL, fenced_ledger_entry_count BIGINT NOT NULL,
  restored_snapshot_digest TEXT NOT NULL, pre_restore_event_root_digest TEXT NOT NULL,
  pre_restore_event_head_count BIGINT NOT NULL, effect_inventory_digest TEXT NOT NULL,
  effect_inventory_count BIGINT NOT NULL, worker_checkpoint_set_digest TEXT NOT NULL,
  worker_checkpoint_count BIGINT NOT NULL, issued_at TIMESTAMPTZ NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL, nonce TEXT NOT NULL, signer_separation_subject TEXT NOT NULL,
  protected_b64 TEXT NOT NULL, payload_b64 TEXT NOT NULL, signature_b64 TEXT NOT NULL,
  signer_key_id_digest TEXT NOT NULL, envelope_digest TEXT NOT NULL,
  verified_at TIMESTAMPTZ NOT NULL DEFAULT now(), authentication_audit_id TEXT NOT NULL,
  nomos_decision_id TEXT NOT NULL, UNIQUE (deployment_name, restore_id),
  UNIQUE (deployment_name, signer_separation_subject, nonce), UNIQUE (deployment_name, envelope_digest),
  FOREIGN KEY (deployment_name, authentication_audit_id)
    REFERENCES okg.external_action_auth_audits(deployment_name, authentication_audit_id) ON DELETE RESTRICT,
  FOREIGN KEY (deployment_name, nomos_decision_id)
    REFERENCES okg.nomos_decisions(deployment_name, decision_id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS okg.external_action_restore_fences (
  deployment_name TEXT PRIMARY KEY, highest_accepted_epoch BIGINT NOT NULL, lease_digest TEXT NOT NULL,
  quarantine_state TEXT NOT NULL CHECK (quarantine_state IN ('clear','blocked')),
  service_mode TEXT NOT NULL CHECK (service_mode IN ('restore_only','normal')),
  lease_chain_digest TEXT NOT NULL, verification_config_digest TEXT NOT NULL,
  verification_login_binding_digest TEXT NOT NULL, restore_attestation_digest TEXT,
  accepted_at TIMESTAMPTZ NOT NULL DEFAULT now(), mode_changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  FOREIGN KEY (deployment_name, restore_attestation_digest)
    REFERENCES okg.external_action_restore_attestations(deployment_name, envelope_digest) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS okg.external_action_restore_sessions (
  restore_session_row_id BIGSERIAL PRIMARY KEY, deployment_name TEXT NOT NULL, restore_id TEXT NOT NULL,
  request_hash TEXT NOT NULL, idempotency_key_hash TEXT NOT NULL,
  operator_separation_subject TEXT NOT NULL, authentication_audit_id TEXT NOT NULL,
  nomos_decision_id TEXT NOT NULL, restore_attestation_envelope_digest TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('quarantined','staging','complete','failed')),
  completion_receipt_envelope_digest TEXT, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), UNIQUE (deployment_name, restore_id),
  FOREIGN KEY (deployment_name, restore_attestation_envelope_digest)
    REFERENCES okg.external_action_restore_attestations(deployment_name, envelope_digest) ON DELETE RESTRICT,
  FOREIGN KEY (deployment_name, authentication_audit_id)
    REFERENCES okg.external_action_auth_audits(deployment_name, authentication_audit_id) ON DELETE RESTRICT,
  FOREIGN KEY (deployment_name, nomos_decision_id)
    REFERENCES okg.nomos_decisions(deployment_name, decision_id) ON DELETE RESTRICT
);

-- Restore staging sessions are immutable. Finalization is represented by an
-- append-only terminal receipt instead of rewriting the staging evidence.
CREATE TABLE IF NOT EXISTS okg.external_action_restore_completion_receipts (
  completion_receipt_row_id BIGSERIAL PRIMARY KEY,
  deployment_name TEXT NOT NULL,
  restore_id TEXT NOT NULL,
  restore_attestation_envelope_digest TEXT NOT NULL,
  completion_receipt_envelope_digest TEXT NOT NULL,
  completed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  authentication_audit_id TEXT NOT NULL,
  nomos_decision_id TEXT NOT NULL,
  store_record JSONB,
  protected_b64 TEXT,
  payload_b64 TEXT,
  signature_b64 TEXT,
  completion_receipt_projection JSONB,
  UNIQUE (deployment_name, restore_id),
  UNIQUE (deployment_name, completion_receipt_envelope_digest),
  FOREIGN KEY (deployment_name, restore_id)
    REFERENCES okg.external_action_restore_sessions(deployment_name, restore_id) ON DELETE RESTRICT,
  FOREIGN KEY (deployment_name, authentication_audit_id)
    REFERENCES okg.external_action_auth_audits(deployment_name, authentication_audit_id) ON DELETE RESTRICT,
  FOREIGN KEY (deployment_name, nomos_decision_id)
    REFERENCES okg.nomos_decisions(deployment_name, decision_id) ON DELETE RESTRICT
);
ALTER TABLE okg.external_action_restore_completion_receipts
  ADD COLUMN IF NOT EXISTS protected_b64 TEXT,
  ADD COLUMN IF NOT EXISTS payload_b64 TEXT,
  ADD COLUMN IF NOT EXISTS signature_b64 TEXT,
  ADD COLUMN IF NOT EXISTS completion_receipt_projection JSONB;

-- The API accepts signed envelope references and a checked projection of the
-- restore inventory.  Keep the projection on the immutable session so the
-- finalizer can compare staged evidence to an authoritative set rather than
-- treating caller-supplied counts as coverage.
ALTER TABLE okg.external_action_restore_sessions
  ADD COLUMN IF NOT EXISTS effect_inventory JSONB NOT NULL DEFAULT '[]'::jsonb,
  ADD COLUMN IF NOT EXISTS effect_inventory_digest TEXT,
  ADD COLUMN IF NOT EXISTS effect_inventory_count BIGINT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS old_epoch_lease_envelope_digest TEXT,
  ADD COLUMN IF NOT EXISTS new_epoch_lease_envelope_digest TEXT,
  ADD COLUMN IF NOT EXISTS worker_quiescence_envelope_digest TEXT,
  ADD COLUMN IF NOT EXISTS provider_authority_fence_envelope_digest TEXT,
  ADD COLUMN IF NOT EXISTS ledger_fence_envelope_digest TEXT;

CREATE TABLE IF NOT EXISTS okg.external_action_restore_finalize_idempotency (
  finalize_idempotency_row_id BIGSERIAL PRIMARY KEY,
  deployment_name TEXT NOT NULL,
  restore_id TEXT NOT NULL,
  operator_separation_subject TEXT NOT NULL,
  idempotency_key_hash TEXT NOT NULL,
  request_hash TEXT NOT NULL,
  response_body_jcs BYTEA NOT NULL,
  completion_receipt_envelope_digest TEXT NOT NULL,
  committed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (deployment_name, restore_id),
  UNIQUE (deployment_name, restore_id, operator_separation_subject, idempotency_key_hash),
  FOREIGN KEY (deployment_name, restore_id)
    REFERENCES okg.external_action_restore_sessions(deployment_name, restore_id)
    ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS okg.external_action_restore_checkpoint_staging (
  checkpoint_staging_row_id BIGSERIAL PRIMARY KEY, deployment_name TEXT NOT NULL,
  restore_id TEXT NOT NULL, batch_index BIGINT NOT NULL, request_hash TEXT NOT NULL,
  checkpoint_envelope_digest TEXT NOT NULL, proof_digest TEXT NOT NULL,
  checkpoint_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  effect_instance_key TEXT,
  operator_separation_subject TEXT,
  idempotency_key_hash TEXT,
  staged_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (deployment_name, restore_id, batch_index, request_hash),
  UNIQUE (deployment_name, restore_id, batch_index, checkpoint_envelope_digest),
  FOREIGN KEY (deployment_name, restore_id)
    REFERENCES okg.external_action_restore_sessions(deployment_name, restore_id) ON DELETE RESTRICT
);

ALTER TABLE okg.external_action_restore_checkpoint_staging
  ADD COLUMN IF NOT EXISTS effect_instance_key TEXT,
  ADD COLUMN IF NOT EXISTS operator_separation_subject TEXT,
  ADD COLUMN IF NOT EXISTS idempotency_key_hash TEXT;
CREATE UNIQUE INDEX IF NOT EXISTS external_action_restore_checkpoint_batch_idx
  ON okg.external_action_restore_checkpoint_staging (deployment_name, restore_id, batch_index);

-- A staging row is a batch receipt/header.  Checkpoints are separate
-- immutable rows so finalization counts entries and checks one-to-one
-- coverage rather than accidentally counting batches.
CREATE TABLE IF NOT EXISTS okg.external_action_restore_checkpoint_entries (
  checkpoint_entry_row_id BIGSERIAL PRIMARY KEY,
  deployment_name TEXT NOT NULL,
  restore_id TEXT NOT NULL,
  batch_index BIGINT NOT NULL CHECK (batch_index >= 0),
  ordinal BIGINT NOT NULL CHECK (ordinal >= 0),
  effect_instance_key TEXT NOT NULL,
  checkpoint_envelope_digest TEXT NOT NULL,
  proof_digest TEXT NOT NULL,
  leaf_index BIGINT CHECK (leaf_index >= 0),
  proof JSONB NOT NULL DEFAULT '{}'::jsonb,
  checkpoint_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  staged_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (deployment_name, restore_id, batch_index, ordinal),
  UNIQUE (deployment_name, restore_id, batch_index, effect_instance_key),
  UNIQUE (deployment_name, restore_id, checkpoint_envelope_digest),
  FOREIGN KEY (deployment_name, restore_id)
    REFERENCES okg.external_action_restore_sessions(deployment_name, restore_id)
    ON DELETE RESTRICT
);
ALTER TABLE okg.external_action_restore_checkpoint_entries
  ADD COLUMN IF NOT EXISTS leaf_index BIGINT,
  ADD COLUMN IF NOT EXISTS proof JSONB NOT NULL DEFAULT '{}'::jsonb;
DO $external_action_restore_checkpoint_leaf_index_check$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_catalog.pg_constraint
     WHERE conrelid='okg.external_action_restore_checkpoint_entries'::pg_catalog.regclass
       AND conname='external_action_restore_checkpoint_entries_leaf_index_check'
  ) THEN
    ALTER TABLE okg.external_action_restore_checkpoint_entries
      ADD CONSTRAINT external_action_restore_checkpoint_entries_leaf_index_check
      CHECK (leaf_index IS NULL OR leaf_index >= 0);
  END IF;
END
$external_action_restore_checkpoint_leaf_index_check$;

CREATE TABLE IF NOT EXISTS okg.external_action_restore_recoveries (
  recovery_row_id BIGSERIAL PRIMARY KEY, deployment_name TEXT NOT NULL, old_epoch BIGINT NOT NULL,
  new_epoch BIGINT NOT NULL, effect_instance_key TEXT NOT NULL, submission_content_hash TEXT NOT NULL,
  checkpoint_digest TEXT NOT NULL, restore_attestation_digest TEXT NOT NULL,
  classification TEXT NOT NULL CHECK (classification IN (
    'database_restore_no_effect_started','database_restore_terminal_review_confirmed',
    'database_restore_effect_uncertain','database_restore_succeeded','database_restore_failed',
    'database_restore_terminal_confirmed','database_restore_terminal_conflict')),
  source_proposal_id TEXT, source_decision_id TEXT,
  source_execution_id TEXT, recovered_review_state TEXT, recovered_execution_state TEXT,
  recovered_outcome TEXT, effective_at TIMESTAMPTZ NOT NULL,
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(), pre_restore_effect_event_head_hash TEXT,
  restored_effect_event_head_hash TEXT, signed_worker_checkpoint_digest TEXT NOT NULL,
  recovery_digest TEXT,
  UNIQUE (deployment_name, effect_instance_key, signed_worker_checkpoint_digest),
  FOREIGN KEY (deployment_name, restore_attestation_digest)
    REFERENCES okg.external_action_restore_attestations(deployment_name, envelope_digest) ON DELETE RESTRICT,
  FOREIGN KEY (deployment_name, source_proposal_id)
    REFERENCES okg.external_action_proposals(deployment_name, proposal_id) ON DELETE RESTRICT,
  FOREIGN KEY (deployment_name, source_decision_id)
    REFERENCES okg.external_action_decisions(deployment_name, decision_id) ON DELETE RESTRICT,
  FOREIGN KEY (source_execution_id)
    REFERENCES okg.external_action_executions(execution_id) ON DELETE RESTRICT
);
ALTER TABLE okg.external_action_restore_recoveries
  ADD COLUMN IF NOT EXISTS recovery_digest TEXT;
DO $external_action_restore_recovery_classification_check$
BEGIN
  ALTER TABLE okg.external_action_restore_recoveries
    DROP CONSTRAINT IF EXISTS external_action_restore_recoveries_classification_check;
  ALTER TABLE okg.external_action_restore_recoveries
    ADD CONSTRAINT external_action_restore_recoveries_classification_check
    CHECK (classification IN (
      'database_restore_no_effect_started','database_restore_terminal_review_confirmed',
      'database_restore_effect_uncertain','database_restore_succeeded','database_restore_failed',
      'database_restore_terminal_confirmed','database_restore_terminal_conflict'));
END
$external_action_restore_recovery_classification_check$;

CREATE TABLE IF NOT EXISTS okg.external_action_api_role_bindings (
  "session_user" TEXT PRIMARY KEY CHECK (btrim("session_user") <> ''),
  deployment_name TEXT NOT NULL CHECK (btrim(deployment_name) <> ''),
  binding_version BIGINT NOT NULL DEFAULT 1, status TEXT NOT NULL DEFAULT 'active'
    CHECK (status IN ('active','disabled')), bound_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  verifier_capability_digest TEXT,
  verifier_capability_generation BIGINT NOT NULL DEFAULT 1,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), UNIQUE ("session_user", deployment_name)
);
ALTER TABLE okg.external_action_api_role_bindings
  ADD COLUMN IF NOT EXISTS verifier_capability_digest TEXT,
  ADD COLUMN IF NOT EXISTS verifier_capability_generation BIGINT NOT NULL DEFAULT 1;

-- The adapter keeps a JSON-shaped copy of each validated domain record for
-- exact restart hydration.  The normalized columns above remain the query and
-- constraint surface; these private columns are absent from API projections.
ALTER TABLE okg.external_action_proposals
  ADD COLUMN IF NOT EXISTS store_record JSONB,
  ADD COLUMN IF NOT EXISTS action_contract_record JSONB,
  ADD COLUMN IF NOT EXISTS submission_idempotency_record JSONB;
ALTER TABLE okg.external_action_payload_versions
  ADD COLUMN IF NOT EXISTS store_record JSONB;
ALTER TABLE okg.external_action_auth_audits
  ADD COLUMN IF NOT EXISTS store_record JSONB,
  ADD COLUMN IF NOT EXISTS provider_status TEXT,
  ADD COLUMN IF NOT EXISTS binding_status TEXT,
  ADD COLUMN IF NOT EXISTS provider_updated_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS binding_updated_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS provider_credential_generation_digest TEXT,
  ADD COLUMN IF NOT EXISTS binding_credential_generation_digest TEXT,
  ADD COLUMN IF NOT EXISTS credential_generation_digest TEXT,
  ADD COLUMN IF NOT EXISTS presented_credential_generation_digest TEXT,
  ADD COLUMN IF NOT EXISTS token_issued_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS certificate_thumbprint TEXT,
  ADD COLUMN IF NOT EXISTS certificate_not_before TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS certificate_not_after TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS verifier_assertion_nonce_digest TEXT,
  ADD COLUMN IF NOT EXISTS verifier_assertion_expires_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS verifier_transition_digest TEXT,
  ADD COLUMN IF NOT EXISTS verifier_policy_input_digest TEXT,
  ADD COLUMN IF NOT EXISTS verifier_required_obligations_digest TEXT,
  ADD COLUMN IF NOT EXISTS verifier_completed_obligations_digest TEXT,
  ADD COLUMN IF NOT EXISTS verifier_missing_obligations_digest TEXT,
  ADD COLUMN IF NOT EXISTS verifier_assertion_tag TEXT,
  ADD COLUMN IF NOT EXISTS verifier_capability_generation BIGINT;
CREATE UNIQUE INDEX IF NOT EXISTS external_action_auth_audits_verifier_nonce_idx
  ON okg.external_action_auth_audits
    (deployment_name, verifier_capability_generation, verifier_assertion_nonce_digest)
  WHERE verifier_assertion_nonce_digest IS NOT NULL;

DO $external_action_auth_audit_checks$
BEGIN
  ALTER TABLE okg.external_action_api_role_bindings
    DROP CONSTRAINT IF EXISTS external_action_api_role_bindings_verifier_check;
  ALTER TABLE okg.external_action_api_role_bindings
    ADD CONSTRAINT external_action_api_role_bindings_verifier_check CHECK (
      verifier_capability_generation > 0
      AND (
        verifier_capability_digest IS NULL
        OR verifier_capability_digest ~ '^sha256:[0-9a-f]{64}$'
      )
    );
  ALTER TABLE okg.external_action_auth_audits
    DROP CONSTRAINT IF EXISTS external_action_auth_audits_provider_status_check;
  ALTER TABLE okg.external_action_auth_audits
    ADD CONSTRAINT external_action_auth_audits_provider_status_check
    CHECK (provider_status IS NULL OR provider_status IN ('active','disabled','rotating','revoked'));
  ALTER TABLE okg.external_action_auth_audits
    DROP CONSTRAINT IF EXISTS external_action_auth_audits_binding_status_check;
  ALTER TABLE okg.external_action_auth_audits
    ADD CONSTRAINT external_action_auth_audits_binding_status_check
    CHECK (binding_status IS NULL OR binding_status IN ('active','disabled'));
  ALTER TABLE okg.external_action_auth_audits
    DROP CONSTRAINT IF EXISTS external_action_auth_audits_provider_credential_generatio_check;
  ALTER TABLE okg.external_action_auth_audits
    DROP CONSTRAINT IF EXISTS external_action_auth_audits_provider_generation_check;
  ALTER TABLE okg.external_action_auth_audits
    ADD CONSTRAINT external_action_auth_audits_provider_generation_check
    CHECK (provider_credential_generation_digest IS NULL OR provider_credential_generation_digest ~ '^sha256:[0-9a-f]{64}$');
  ALTER TABLE okg.external_action_auth_audits
    DROP CONSTRAINT IF EXISTS external_action_auth_audits_binding_credential_generation_check;
  ALTER TABLE okg.external_action_auth_audits
    DROP CONSTRAINT IF EXISTS external_action_auth_audits_binding_generation_check;
  ALTER TABLE okg.external_action_auth_audits
    ADD CONSTRAINT external_action_auth_audits_binding_generation_check
    CHECK (binding_credential_generation_digest IS NULL OR binding_credential_generation_digest ~ '^sha256:[0-9a-f]{64}$');
  ALTER TABLE okg.external_action_auth_audits
    DROP CONSTRAINT IF EXISTS external_action_auth_audits_credential_generation_digest_check;
  ALTER TABLE okg.external_action_auth_audits
    DROP CONSTRAINT IF EXISTS external_action_auth_audits_credential_generation_check;
  ALTER TABLE okg.external_action_auth_audits
    ADD CONSTRAINT external_action_auth_audits_credential_generation_check
    CHECK (credential_generation_digest IS NULL OR credential_generation_digest ~ '^sha256:[0-9a-f]{64}$');
  ALTER TABLE okg.external_action_auth_audits
    DROP CONSTRAINT IF EXISTS external_action_auth_audits_presented_credential_generati_check;
  ALTER TABLE okg.external_action_auth_audits
    DROP CONSTRAINT IF EXISTS external_action_auth_audits_presented_generation_check;
  ALTER TABLE okg.external_action_auth_audits
    ADD CONSTRAINT external_action_auth_audits_presented_generation_check
    CHECK (presented_credential_generation_digest IS NULL OR presented_credential_generation_digest ~ '^sha256:[0-9a-f]{64}$');
  ALTER TABLE okg.external_action_auth_audits
    DROP CONSTRAINT IF EXISTS external_action_auth_audits_certificate_thumbprint_check;
  ALTER TABLE okg.external_action_auth_audits
    ADD CONSTRAINT external_action_auth_audits_certificate_thumbprint_check
    CHECK (certificate_thumbprint IS NULL OR (btrim(certificate_thumbprint) = certificate_thumbprint AND octet_length(certificate_thumbprint) BETWEEN 1 AND 512));
  ALTER TABLE okg.external_action_auth_audits
    DROP CONSTRAINT IF EXISTS external_action_auth_audits_certificate_interval_check;
  ALTER TABLE okg.external_action_auth_audits
    ADD CONSTRAINT external_action_auth_audits_certificate_interval_check
    CHECK (certificate_not_before IS NULL OR certificate_not_after IS NULL OR certificate_not_after >= certificate_not_before);
END
$external_action_auth_audit_checks$;

CREATE OR REPLACE FUNCTION okg.external_action_api_role_binding_guard()
RETURNS TRIGGER
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $external_action_api_role_binding_guard$
BEGIN
  IF TG_OP = 'INSERT' THEN
    IF NEW.binding_version <> 1
       OR NEW.verifier_capability_generation <> 1 THEN
      RAISE EXCEPTION 'external action API binding must start at version and capability generation one'
        USING ERRCODE = '22023';
    END IF;
    RETURN NEW;
  END IF;
  IF NEW."session_user" IS DISTINCT FROM OLD."session_user"
     OR NEW.deployment_name IS DISTINCT FROM OLD.deployment_name
     OR NEW.binding_version IS DISTINCT FROM OLD.binding_version
     OR NEW.bound_at IS DISTINCT FROM OLD.bound_at THEN
    RAISE EXCEPTION 'external action API binding identity is immutable'
      USING ERRCODE = '55006';
  END IF;
  IF OLD.verifier_capability_digest IS NULL
     AND NEW.verifier_capability_digest IS NOT NULL THEN
    IF NEW.verifier_capability_generation <> OLD.verifier_capability_generation THEN
      RAISE EXCEPTION 'initial external action verifier provisioning cannot change generation'
        USING ERRCODE = '22023';
    END IF;
  ELSIF NEW.verifier_capability_digest IS DISTINCT FROM
        OLD.verifier_capability_digest THEN
    IF NEW.verifier_capability_digest IS NULL
       OR NEW.verifier_capability_generation <>
            OLD.verifier_capability_generation + 1 THEN
      RAISE EXCEPTION 'external action verifier rotation must replace the digest and advance one generation'
        USING ERRCODE = '22023';
    END IF;
  ELSIF NEW.verifier_capability_generation IS DISTINCT FROM
        OLD.verifier_capability_generation THEN
    RAISE EXCEPTION 'external action verifier generation requires a new capability digest'
      USING ERRCODE = '22023';
  END IF;
  NEW.updated_at := clock_timestamp();
  RETURN NEW;
END
$external_action_api_role_binding_guard$;
REVOKE ALL ON FUNCTION okg.external_action_api_role_binding_guard() FROM PUBLIC;
DROP TRIGGER IF EXISTS external_action_api_role_binding_guard_trigger
  ON okg.external_action_api_role_bindings;
CREATE TRIGGER external_action_api_role_binding_guard_trigger
BEFORE INSERT OR UPDATE ON okg.external_action_api_role_bindings
FOR EACH ROW EXECUTE FUNCTION okg.external_action_api_role_binding_guard();

ALTER TABLE okg.external_action_decisions
  ADD COLUMN IF NOT EXISTS store_record JSONB;
ALTER TABLE okg.external_action_transition_idempotency
  ADD COLUMN IF NOT EXISTS store_record JSONB;
ALTER TABLE okg.external_action_executions
  ADD COLUMN IF NOT EXISTS store_record JSONB;
ALTER TABLE okg.external_action_owner_unavailability_attestations
  ADD COLUMN IF NOT EXISTS store_record JSONB;
ALTER TABLE okg.external_action_outcomes
  ADD COLUMN IF NOT EXISTS store_record JSONB;
ALTER TABLE okg.external_action_events
  ADD COLUMN IF NOT EXISTS store_record JSONB;
ALTER TABLE okg.external_action_execution_epoch_leases
  ADD COLUMN IF NOT EXISTS store_record JSONB;
ALTER TABLE okg.external_action_restore_attestations
  ADD COLUMN IF NOT EXISTS store_record JSONB;
ALTER TABLE okg.external_action_restore_fences
  ADD COLUMN IF NOT EXISTS store_record JSONB;
ALTER TABLE okg.external_action_restore_sessions
  ADD COLUMN IF NOT EXISTS store_record JSONB;
ALTER TABLE okg.external_action_restore_checkpoint_staging
  ADD COLUMN IF NOT EXISTS store_record JSONB;
ALTER TABLE okg.external_action_restore_recoveries
  ADD COLUMN IF NOT EXISTS store_record JSONB;

CREATE INDEX IF NOT EXISTS external_action_proposals_review_idx
  ON okg.external_action_proposals (deployment_name, review_state, updated_at DESC);
CREATE INDEX IF NOT EXISTS external_action_events_effect_idx
  ON okg.external_action_events (deployment_name, effect_instance_key, sequence);
CREATE INDEX IF NOT EXISTS external_action_api_role_bindings_deployment_idx
  ON okg.external_action_api_role_bindings (deployment_name, status);
CREATE INDEX IF NOT EXISTS external_action_auth_audits_binding_idx
  ON okg.external_action_auth_audits
    (deployment_name, nomos_provider_id, nomos_subject_binding_id, observed_at DESC);

CREATE OR REPLACE FUNCTION okg.external_action_reject_mutation()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog, okg AS $$
BEGIN
  RAISE EXCEPTION 'external action relation % is append-only', TG_TABLE_NAME
    USING ERRCODE = '55006';
END
$$;

DO $external_action_append_only_triggers$
DECLARE relation_name TEXT;
BEGIN
  FOREACH relation_name IN ARRAY ARRAY[
    'external_action_payload_versions','external_action_decisions',
    'external_action_transition_idempotency','external_action_executions',
    'external_action_outcomes','external_action_events','external_action_auth_audits',
    'external_action_dpop_replay_nonces','external_action_owner_unavailability_attestations',
    'external_action_execution_epoch_leases','external_action_restore_attestations',
    'external_action_restore_sessions','external_action_restore_checkpoint_staging',
    'external_action_restore_completion_receipts',
    'external_action_restore_finalize_idempotency',
    'external_action_restore_checkpoint_entries',
    'external_action_restore_recoveries'
  ] LOOP
    EXECUTE format('DROP TRIGGER IF EXISTS %I_append_only ON okg.%I', relation_name, relation_name);
    EXECUTE format('CREATE TRIGGER %I_append_only BEFORE UPDATE OR DELETE ON okg.%I FOR EACH STATEMENT EXECUTE FUNCTION okg.external_action_reject_mutation()', relation_name, relation_name);
  END LOOP;
END
$external_action_append_only_triggers$;

CREATE OR REPLACE FUNCTION okg.external_action_monotonic_nomos_updated_at()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog, okg AS $$
BEGIN
  -- `now()` is transaction-stable, so disable -> re-enable in one
  -- transaction could otherwise reuse the pre-rotation barrier.  Advance
  -- from the old value by one microsecond and use the wall clock as the
  -- floor; every committed identity mutation therefore invalidates older
  -- credentials, even when several mutations share a transaction.
  NEW.updated_at := GREATEST(
    COALESCE(OLD.updated_at, '-infinity'::timestamptz) + interval '1 microsecond',
    clock_timestamp()
  );
  RETURN NEW;
END
$$;

DROP TRIGGER IF EXISTS external_action_identity_provider_updated_at ON okg.nomos_identity_providers;
CREATE TRIGGER external_action_identity_provider_updated_at
  BEFORE UPDATE ON okg.nomos_identity_providers FOR EACH ROW
  EXECUTE FUNCTION okg.external_action_monotonic_nomos_updated_at();
DROP TRIGGER IF EXISTS external_action_subject_binding_updated_at ON okg.nomos_subject_bindings;
CREATE TRIGGER external_action_subject_binding_updated_at
  BEFORE UPDATE ON okg.nomos_subject_bindings FOR EACH ROW
  EXECUTE FUNCTION okg.external_action_monotonic_nomos_updated_at();
DROP TRIGGER IF EXISTS external_action_policy_bundle_updated_at ON okg.nomos_policy_bundles;
CREATE TRIGGER external_action_policy_bundle_updated_at
  BEFORE UPDATE ON okg.nomos_policy_bundles FOR EACH ROW
  EXECUTE FUNCTION okg.external_action_monotonic_nomos_updated_at();

-- Policy and identity mutations share the deployment authority lock with
-- lifecycle commands.  A transition therefore cannot validate an allow and
-- commit across a concurrent authority change.
CREATE OR REPLACE FUNCTION okg.external_action_nomos_authority_lock()
RETURNS TRIGGER
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $external_action_nomos_authority_lock$
DECLARE
  mutation_deployment TEXT;
BEGIN
  IF TG_OP = 'DELETE' THEN
    mutation_deployment := OLD.deployment_name;
  ELSE
    mutation_deployment := NEW.deployment_name;
  END IF;
  IF mutation_deployment IS NULL OR btrim(mutation_deployment) = '' THEN
    RAISE EXCEPTION 'Nomos authority mutation has no deployment'
      USING ERRCODE = '22023';
  END IF;
  IF TG_OP = 'UPDATE'
     AND OLD.deployment_name IS DISTINCT FROM NEW.deployment_name THEN
    RAISE EXCEPTION 'Nomos authority deployment is immutable'
      USING ERRCODE = '22023';
  END IF;
  IF TG_OP = 'UPDATE'
     AND TG_TABLE_NAME = 'nomos_policy_bundles'
     AND (
       to_jsonb(OLD) - ARRAY[
         'status','effective_from','effective_until','metadata','updated_at'
       ]::text[]
     ) IS DISTINCT FROM (
       to_jsonb(NEW) - ARRAY[
         'status','effective_from','effective_until','metadata','updated_at'
       ]::text[]
     ) THEN
    RAISE EXCEPTION 'Nomos policy content is immutable for a retained fingerprint'
      USING ERRCODE = '22023';
  END IF;
  PERFORM pg_advisory_xact_lock(
    hashtextextended(
      'okg.external_action.deployment:' || mutation_deployment, 0
    )
  );
  IF TG_OP = 'DELETE' THEN
    RETURN OLD;
  END IF;
  RETURN NEW;
END
$external_action_nomos_authority_lock$;

DROP TRIGGER IF EXISTS external_action_nomos_policy_bundle_authority_lock
  ON okg.nomos_policy_bundles;
CREATE TRIGGER external_action_nomos_policy_bundle_authority_lock
  BEFORE INSERT OR UPDATE OR DELETE ON okg.nomos_policy_bundles
  FOR EACH ROW EXECUTE FUNCTION okg.external_action_nomos_authority_lock();
DROP TRIGGER IF EXISTS external_action_nomos_policy_activation_authority_lock
  ON okg.nomos_policy_activations;
CREATE TRIGGER external_action_nomos_policy_activation_authority_lock
  BEFORE INSERT OR UPDATE OR DELETE ON okg.nomos_policy_activations
  FOR EACH ROW EXECUTE FUNCTION okg.external_action_nomos_authority_lock();
DROP TRIGGER IF EXISTS external_action_nomos_identity_provider_authority_lock
  ON okg.nomos_identity_providers;
CREATE TRIGGER external_action_nomos_identity_provider_authority_lock
  BEFORE INSERT OR UPDATE OR DELETE ON okg.nomos_identity_providers
  FOR EACH ROW EXECUTE FUNCTION okg.external_action_nomos_authority_lock();
DROP TRIGGER IF EXISTS external_action_nomos_subject_binding_authority_lock
  ON okg.nomos_subject_bindings;
CREATE TRIGGER external_action_nomos_subject_binding_authority_lock
  BEFORE INSERT OR UPDATE OR DELETE ON okg.nomos_subject_bindings
  FOR EACH ROW EXECUTE FUNCTION okg.external_action_nomos_authority_lock();

CREATE OR REPLACE FUNCTION okg.external_action_session_deployment()
RETURNS TEXT LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, okg AS $$
DECLARE resolved_session_user TEXT := session_user; resolved_deployment TEXT; binding_count INTEGER;
BEGIN
  SELECT count(*)::integer, min(b.deployment_name) INTO binding_count, resolved_deployment
    FROM okg.external_action_api_role_bindings b
   WHERE b."session_user" = resolved_session_user AND b.status = 'active';
  IF binding_count <> 1 OR resolved_deployment IS NULL THEN
    RAISE EXCEPTION 'external action API login has no unique active deployment binding' USING ERRCODE = '28000';
  END IF;
  RETURN resolved_deployment;
END
$$;

-- Startup hashes bind to the database's system identifier and the exact
-- session-user row selected under a row lock.  Callers cannot substitute a
-- login or deployment projection from configuration.
CREATE OR REPLACE FUNCTION okg.external_action_startup_login_binding()
RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $external_action_startup_login_binding$
DECLARE
  resolved_session_user TEXT := session_user;
  bound_deployment TEXT := okg.external_action_session_deployment();
  binding_row okg.external_action_api_role_bindings%ROWTYPE;
  system_identifier TEXT;
BEGIN
  SELECT * INTO binding_row
    FROM okg.external_action_api_role_bindings
   WHERE "session_user" = resolved_session_user
     AND deployment_name = bound_deployment
     AND status = 'active'
   FOR SHARE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'external action API login has no active deployment binding'
      USING ERRCODE = '28000';
  END IF;
  SELECT c.system_identifier::TEXT INTO system_identifier
    FROM pg_control_system() AS c;
  RETURN jsonb_build_object(
    'database_system_identifier', system_identifier,
    'session_user', binding_row."session_user",
    'deployment', binding_row.deployment_name,
    'binding_version', binding_row.binding_version,
    'verifier_capability_generation', binding_row.verifier_capability_generation,
    'bound_at', to_char(
      binding_row.bound_at AT TIME ZONE 'UTC',
      'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
    )
  );
END
$external_action_startup_login_binding$;
REVOKE ALL ON FUNCTION okg.external_action_startup_login_binding() FROM PUBLIC;

-- Return only the database-owned claim-owner availability projection used by
-- the service preflight.  The execution's retained authentication audit is
-- the identity source; callers cannot provide provider or subject fields.
CREATE OR REPLACE FUNCTION okg.external_action_claim_owner_unavailability(p_execution_id TEXT)
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, okg AS $external_action_claim_owner_unavailability$
DECLARE
  bound_deployment TEXT := okg.external_action_session_deployment(); observed_at TIMESTAMPTZ := clock_timestamp();
  execution_row okg.external_action_executions%ROWTYPE;
  claim_auth_provider_id TEXT; claim_auth_binding_id TEXT; claim_auth_subject TEXT;
  claim_provider_status TEXT; claim_provider_updated_at TIMESTAMPTZ;
  claim_binding_status TEXT; claim_binding_updated_at TIMESTAMPTZ; claim_binding_subject TEXT;
  revocation_updated_at TIMESTAMPTZ; active_replacement BOOLEAN; unavailable BOOLEAN;
BEGIN
  PERFORM okg.external_action_verifier_capability_status();
  IF p_execution_id IS NULL OR btrim(p_execution_id)='' THEN RAISE EXCEPTION 'external action execution id is required' USING ERRCODE='22023'; END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended('okg.external_action.deployment:'||bound_deployment,0));
  SELECT * INTO execution_row FROM okg.external_action_executions WHERE deployment_name=bound_deployment AND execution_id=p_execution_id FOR SHARE;
  IF execution_row.execution_id IS NULL THEN RAISE EXCEPTION 'external action execution was not found' USING ERRCODE='P0002'; END IF;
  SELECT a.nomos_provider_id,a.nomos_subject_binding_id,a.separation_subject,p.status,p.updated_at,b.status,b.updated_at,b.subject
    INTO claim_auth_provider_id,claim_auth_binding_id,claim_auth_subject,claim_provider_status,claim_provider_updated_at,claim_binding_status,claim_binding_updated_at,claim_binding_subject
    FROM okg.external_action_auth_audits a
    JOIN okg.nomos_identity_providers p ON p.deployment_name=a.deployment_name AND p.provider_id=a.nomos_provider_id
    JOIN okg.nomos_subject_bindings b ON b.deployment_name=a.deployment_name AND b.binding_id=a.nomos_subject_binding_id AND b.provider_id=p.provider_id AND b.issuer=p.issuer AND b.subject=a.separation_subject
   WHERE a.deployment_name=bound_deployment AND a.authentication_audit_id=execution_row.authentication_audit_id AND a.separation_subject=execution_row.worker_separation_subject;
  IF claim_auth_provider_id IS NULL OR claim_auth_binding_id IS NULL OR claim_auth_subject IS DISTINCT FROM execution_row.worker_separation_subject OR claim_binding_subject IS DISTINCT FROM execution_row.worker_separation_subject THEN RAISE EXCEPTION 'claim owner identity authority is unavailable' USING ERRCODE='42501'; END IF;
  active_replacement := EXISTS (SELECT 1 FROM okg.nomos_identity_providers p JOIN okg.nomos_subject_bindings b ON b.deployment_name=p.deployment_name AND b.provider_id=p.provider_id AND b.issuer=p.issuer WHERE p.deployment_name=bound_deployment AND b.subject=execution_row.worker_separation_subject AND NOT (p.provider_id=claim_auth_provider_id AND b.binding_id=claim_auth_binding_id) AND p.status='active' AND b.status='active' AND (p.effective_from IS NULL OR p.effective_from<=observed_at) AND (p.effective_until IS NULL OR observed_at<p.effective_until) AND (b.effective_from IS NULL OR b.effective_from<=observed_at) AND (b.effective_until IS NULL OR observed_at<b.effective_until));
  revocation_updated_at := CASE WHEN claim_provider_status IN ('disabled','revoked') AND claim_provider_updated_at>execution_row.claimed_at THEN claim_provider_updated_at WHEN claim_binding_status IN ('disabled','revoked') AND claim_binding_updated_at>execution_row.claimed_at THEN claim_binding_updated_at ELSE GREATEST(claim_provider_updated_at,claim_binding_updated_at) END;
  unavailable := ((claim_provider_status IN ('disabled','revoked') AND claim_provider_updated_at>execution_row.claimed_at) OR (claim_binding_status IN ('disabled','revoked') AND claim_binding_updated_at>execution_row.claimed_at)) AND NOT active_replacement;
  RETURN jsonb_build_object('authentication_audit_id',execution_row.authentication_audit_id,'unavailable',unavailable,'updated_at',to_char(revocation_updated_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),'active_replacement',active_replacement);
END
$external_action_claim_owner_unavailability$;
REVOKE ALL ON FUNCTION okg.external_action_claim_owner_unavailability(TEXT) FROM PUBLIC;

CREATE OR REPLACE FUNCTION okg.external_action_assert_deployment(p_deployment TEXT)
RETURNS BOOLEAN LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, okg AS $$
DECLARE bound_deployment TEXT;
BEGIN
  bound_deployment := okg.external_action_session_deployment();
  IF p_deployment IS NULL OR p_deployment <> bound_deployment THEN
    RAISE EXCEPTION 'external action deployment is not bound to session_user' USING ERRCODE = '28000';
  END IF;
  RETURN TRUE;
END
$$;

-- The standalone review service is deliberately not granted raw-table access.
-- These fixed entry points are the only write/read bridge used by its
-- Postgres adapter.  ``p_kind`` is a closed enum below; it is never a table
-- name and is never interpolated into SQL.  The function owner is provisioned
-- by views.sql; the conditional grants make this migration safe when it is
-- replayed before that role is created.
-- Fixed semantic command boundary.  The JSON command is a value, never a
-- relation name or SQL fragment.  Every public command validates its closed
-- object shape before reading any field; all SQL below is static.
CREATE OR REPLACE FUNCTION okg.external_action_require_command(
  p_command JSONB,
  p_allowed TEXT[],
  p_required TEXT[]
) RETURNS VOID
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $external_action_require_command$
DECLARE
  command_key TEXT;
  required_key TEXT;
  audit_row okg.external_action_auth_audits%ROWTYPE;
  actor_subject TEXT;
  actor_principal TEXT;
BEGIN
  IF jsonb_typeof(p_command) IS DISTINCT FROM 'object' THEN
    RAISE EXCEPTION 'external action command must be a JSON object'
      USING ERRCODE = '22023';
  END IF;
  FOR command_key IN SELECT jsonb_object_keys(p_command) LOOP
    IF NOT (command_key = ANY (p_allowed)) THEN
      RAISE EXCEPTION 'external action command contains an unknown field'
        USING ERRCODE = '22023';
    END IF;
  END LOOP;
  FOREACH required_key IN ARRAY p_required LOOP
    IF NOT (p_command ? required_key)
       OR p_command -> required_key IS NULL
       OR p_command -> required_key = 'null'::jsonb THEN
      RAISE EXCEPTION 'external action command is missing a required field'
        USING ERRCODE = '22023';
    END IF;
  END LOOP;
  -- Authenticated lifecycle actors are projections of the retained audit,
  -- never claims supplied by a mapped login.  Auth-audit creation itself is
  -- the one command that necessarily precedes this lookup.
  IF p_command ? 'authentication_audit_id'
     AND p_command->>'authentication_audit_id' IS NOT NULL
     AND NOT (p_command ? 'request_id') THEN
    SELECT * INTO audit_row
      FROM okg.external_action_auth_audits
     WHERE deployment_name = okg.external_action_session_deployment()
       AND authentication_audit_id = p_command->>'authentication_audit_id';
    IF audit_row.authentication_audit_id IS NULL THEN
      RAISE EXCEPTION 'authentication audit is unavailable for lifecycle actor'
        USING ERRCODE = '28000';
    END IF;
    FOREACH actor_subject IN ARRAY ARRAY[
      p_command->>'proposer_separation_subject',
      p_command->>'actor_separation_subject',
      p_command->>'worker_separation_subject',
      p_command->>'worker_or_reconciler_separation_subject',
      p_command->>'operator_separation_subject'
    ] LOOP
      IF actor_subject IS NOT NULL
         AND actor_subject IS DISTINCT FROM audit_row.separation_subject THEN
        RAISE EXCEPTION 'lifecycle actor is not the authenticated separation subject'
          USING ERRCODE = '42501';
      END IF;
    END LOOP;
    FOREACH actor_principal IN ARRAY ARRAY[
      p_command->>'proposer_principal',
      p_command->>'actor_principal',
      p_command->>'worker_principal'
    ] LOOP
      IF actor_principal IS NOT NULL
         AND actor_principal IS DISTINCT FROM audit_row.principal THEN
        RAISE EXCEPTION 'lifecycle principal is not the authenticated principal'
          USING ERRCODE = '42501';
      END IF;
    END LOOP;
  END IF;
END
$external_action_require_command$;

CREATE OR REPLACE FUNCTION okg.external_action_json_bytes(p_value JSONB)
RETURNS BYTEA
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $external_action_json_bytes$
BEGIN
  IF p_value IS NULL OR p_value = 'null'::jsonb THEN
    RETURN NULL;
  ELSIF jsonb_typeof(p_value) = 'object'
        AND p_value ? '__okg_bytes_b64' THEN
    RETURN decode(p_value ->> '__okg_bytes_b64', 'base64');
  ELSIF jsonb_typeof(p_value) = 'string' THEN
    RETURN convert_to(p_value #>> '{}', 'utf8');
  END IF;
  RETURN convert_to(p_value::text, 'utf8');
END
$external_action_json_bytes$;

-- Hash only a caller-independent, closed JSON projection.  The separator is
-- part of the contract and keeps adjacent domains unambiguous.  The
-- projection builders below include every closed event/decision field,
-- including JSON null for absent optional values.
CREATE OR REPLACE FUNCTION okg.external_action_hash(p_domain TEXT, p_value JSONB)
RETURNS TEXT
LANGUAGE SQL IMMUTABLE PARALLEL SAFE
SET search_path = pg_catalog, okg
AS $external_action_hash$
  SELECT 'sha256:' || encode(
    sha256(
      convert_to(p_domain, 'UTF8')
      || decode('00', 'hex')
      || convert_to(okg.jsonb_canonical_text(p_value), 'UTF8')
    ),
    'hex'
  )
$external_action_hash$;
REVOKE ALL ON FUNCTION okg.external_action_hash(TEXT, JSONB) FROM PUBLIC;

-- The external-action HTTP verifier holds a random 256-bit capability which
-- is independent of the mapped PostgreSQL login. Only its SHA-256 digest and
-- generation are retained in the deployment login binding. The raw value is
-- supplied as a transaction-local parameter and is never returned or stored.
CREATE OR REPLACE FUNCTION okg.external_action_hmac_sha256(
  p_key BYTEA,
  p_message BYTEA
) RETURNS TEXT
LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE
SET search_path = pg_catalog, okg
AS $external_action_hmac_sha256$
DECLARE
  key_block BYTEA;
  inner_pad BYTEA;
  outer_pad BYTEA;
  index_value INTEGER;
BEGIN
  IF octet_length(p_key) > 64 THEN
    p_key := sha256(p_key);
  END IF;
  key_block := p_key || decode(repeat('00', 64 - octet_length(p_key)), 'hex');
  inner_pad := key_block;
  outer_pad := key_block;
  FOR index_value IN 0..63 LOOP
    inner_pad := set_byte(
      inner_pad, index_value, get_byte(key_block, index_value) # 54
    );
    outer_pad := set_byte(
      outer_pad, index_value, get_byte(key_block, index_value) # 92
    );
  END LOOP;
  RETURN 'sha256:' || encode(
    sha256(outer_pad || sha256(inner_pad || p_message)), 'hex'
  );
END
$external_action_hmac_sha256$;
REVOKE ALL ON FUNCTION okg.external_action_hmac_sha256(BYTEA, BYTEA) FROM PUBLIC;

CREATE OR REPLACE FUNCTION okg.external_action_verifier_capability_status()
RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $external_action_verifier_capability_status$
DECLARE
  bound_deployment TEXT := okg.external_action_session_deployment();
  binding_row okg.external_action_api_role_bindings%ROWTYPE;
  capability_value TEXT := current_setting(
    'okg.external_action_verifier_capability', true
  );
  capability_digest TEXT;
BEGIN
  SELECT * INTO binding_row
    FROM okg.external_action_api_role_bindings
   WHERE "session_user" = session_user
     AND deployment_name = bound_deployment
     AND status = 'active'
   FOR SHARE;
  IF binding_row."session_user" IS NULL
     OR capability_value IS NULL
     OR capability_value !~ '^[0-9a-f]{64}$' THEN
    RAISE EXCEPTION 'external action verifier capability is unavailable'
      USING ERRCODE = '28000';
  END IF;
  capability_digest := 'sha256:' || encode(
    sha256(decode(capability_value, 'hex')), 'hex'
  );
  IF binding_row.verifier_capability_digest IS DISTINCT FROM capability_digest
     OR binding_row.verifier_capability_generation IS NULL
     OR binding_row.verifier_capability_generation <= 0 THEN
    RAISE EXCEPTION 'external action verifier capability is unavailable'
      USING ERRCODE = '28000';
  END IF;
  RETURN jsonb_build_object(
    'deployment', bound_deployment,
    'capability_generation', binding_row.verifier_capability_generation
  );
END
$external_action_verifier_capability_status$;
REVOKE ALL ON FUNCTION okg.external_action_verifier_capability_status() FROM PUBLIC;

CREATE OR REPLACE FUNCTION okg.external_action_verify_verifier_assertion(
  p_assertion JSONB,
  p_assertion_tag TEXT,
  p_expected JSONB
) RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $external_action_verify_verifier_assertion$
DECLARE
  capability_status JSONB;
  capability_value TEXT := current_setting(
    'okg.external_action_verifier_capability', true
  );
  expected_tag TEXT;
  expected_tag_bytes BYTEA;
  supplied_tag_bytes BYTEA;
  tag_difference INTEGER := 0;
  tag_index INTEGER;
  issued_value TIMESTAMPTZ;
  expires_value TIMESTAMPTZ;
  observed_at TIMESTAMPTZ := clock_timestamp();
  digest_key TEXT;
BEGIN
  IF jsonb_typeof(p_assertion) IS DISTINCT FROM 'object'
     OR EXISTS (
       SELECT 1 FROM jsonb_object_keys(p_assertion) AS key_name
        WHERE key_name NOT IN (
          'schema','deployment','capability_generation',
          'authentication_audit_id','operation','transition_digest',
          'credential_evidence_digest','nonce_digest','issued_at','expires_at',
          'nomos_decision_id','policy_fingerprint','provider_id',
          'provider_status','provider_updated_at','binding_id','binding_status',
          'binding_updated_at','nomos_input_hash','allowed','final_effect',
          'required_obligations_digest','completed_obligations_digest',
          'missing_obligations_digest'
        )
     )
     OR EXISTS (
       SELECT 1 FROM unnest(ARRAY[
         'schema','deployment','capability_generation',
         'authentication_audit_id','operation','transition_digest',
         'credential_evidence_digest','nonce_digest','issued_at','expires_at',
         'nomos_decision_id','policy_fingerprint','provider_id',
         'provider_status','provider_updated_at','binding_id','binding_status',
         'binding_updated_at','nomos_input_hash','allowed','final_effect',
         'required_obligations_digest','completed_obligations_digest',
         'missing_obligations_digest'
       ]) AS required_key
       WHERE NOT (p_assertion ? required_key)
          OR p_assertion -> required_key = 'null'::jsonb
     ) THEN
    RAISE EXCEPTION 'external action verifier assertion shape is invalid'
      USING ERRCODE = '28000';
  END IF;
  IF p_assertion ->> 'schema' <> 'okg.external_action.verifier_assertion/v1'
     OR jsonb_typeof(p_assertion -> 'capability_generation') <> 'number'
     OR jsonb_typeof(p_assertion -> 'allowed') <> 'boolean'
     OR p_assertion ->> 'allowed' <> 'true'
     OR p_assertion ->> 'final_effect' <> 'allow'
     OR p_assertion_tag !~ '^sha256:[0-9a-f]{64}$'
     OR jsonb_typeof(p_expected) <> 'object'
     OR NOT (p_assertion @> p_expected) THEN
    RAISE EXCEPTION 'external action verifier assertion is invalid'
      USING ERRCODE = '28000';
  END IF;
  FOREACH digest_key IN ARRAY ARRAY[
    'transition_digest','credential_evidence_digest','nonce_digest',
    'policy_fingerprint','nomos_input_hash','required_obligations_digest',
    'completed_obligations_digest','missing_obligations_digest'
  ] LOOP
    IF p_assertion ->> digest_key !~ '^sha256:[0-9a-f]{64}$' THEN
      RAISE EXCEPTION 'external action verifier assertion digest is invalid'
        USING ERRCODE = '28000';
    END IF;
  END LOOP;
  IF p_assertion ->> 'issued_at'
       !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[.][0-9]{6}Z$'
     OR p_assertion ->> 'expires_at'
       !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[.][0-9]{6}Z$' THEN
    RAISE EXCEPTION 'external action verifier assertion time is invalid'
      USING ERRCODE = '28000';
  END IF;
  issued_value := (p_assertion ->> 'issued_at')::timestamptz;
  expires_value := (p_assertion ->> 'expires_at')::timestamptz;
  IF issued_value > observed_at + interval '5 seconds'
     OR issued_value < observed_at - interval '35 seconds'
     OR expires_value <= observed_at
     OR expires_value > issued_value + interval '30 seconds' THEN
    RAISE EXCEPTION 'external action verifier assertion is stale'
      USING ERRCODE = '28000';
  END IF;
  capability_status := okg.external_action_verifier_capability_status();
  IF p_assertion ->> 'deployment' IS DISTINCT FROM
       capability_status ->> 'deployment'
     OR (p_assertion ->> 'capability_generation')::bigint IS DISTINCT FROM
       (capability_status ->> 'capability_generation')::bigint THEN
    RAISE EXCEPTION 'external action verifier assertion generation is stale'
      USING ERRCODE = '28000';
  END IF;
  expected_tag := okg.external_action_hmac_sha256(
    decode(capability_value, 'hex'),
    convert_to('okg-external-action-verifier-assertion-v1', 'UTF8')
      || decode('00', 'hex')
      || convert_to(okg.jsonb_canonical_text(p_assertion), 'UTF8')
  );
  expected_tag_bytes := convert_to(expected_tag, 'UTF8');
  supplied_tag_bytes := convert_to(p_assertion_tag, 'UTF8');
  IF octet_length(expected_tag_bytes) <> octet_length(supplied_tag_bytes) THEN
    RAISE EXCEPTION 'external action verifier assertion authentication failed'
      USING ERRCODE = '28000';
  END IF;
  FOR tag_index IN 0..octet_length(expected_tag_bytes) - 1 LOOP
    tag_difference := tag_difference
      | (get_byte(expected_tag_bytes, tag_index)
         # get_byte(supplied_tag_bytes, tag_index));
  END LOOP;
  IF tag_difference <> 0 THEN
    RAISE EXCEPTION 'external action verifier assertion authentication failed'
      USING ERRCODE = '28000';
  END IF;
  RETURN p_assertion;
END
$external_action_verify_verifier_assertion$;
REVOKE ALL ON FUNCTION okg.external_action_verify_verifier_assertion(JSONB, TEXT, JSONB) FROM PUBLIC;

-- Provider idempotency keys use the same closed effect projection as their
-- audit digest, but retain the contract's unpadded base64url representation.
CREATE OR REPLACE FUNCTION okg.external_action_effect_key(p_value JSONB)
RETURNS TEXT
LANGUAGE SQL IMMUTABLE PARALLEL SAFE
SET search_path = pg_catalog, okg
AS $external_action_effect_key$
  SELECT replace(replace(rtrim(encode(
    sha256(
      convert_to('okg-external-action-effect-v1', 'UTF8')
      || decode('00', 'hex')
      || convert_to(okg.jsonb_canonical_text(p_value), 'UTF8')
    ), 'base64'), '='), '+', '-'), '/', '_')
$external_action_effect_key$;
REVOKE ALL ON FUNCTION okg.external_action_effect_key(JSONB) FROM PUBLIC;

CREATE OR REPLACE FUNCTION okg.external_action_nomos_authority_snapshot(
  p_deployment TEXT,
  p_authentication_audit_id TEXT,
  p_nomos_decision_id TEXT,
  p_expected_action TEXT,
  p_expected_transition_digest TEXT
) RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $external_action_nomos_authority_snapshot$
DECLARE
  bound_deployment TEXT := okg.external_action_session_deployment();
  audit_row okg.external_action_auth_audits%ROWTYPE;
  decision_row okg.nomos_decisions%ROWTYPE;
  provider_row okg.nomos_identity_providers%ROWTYPE;
  binding_row okg.nomos_subject_bindings%ROWTYPE;
  login_binding_row okg.external_action_api_role_bindings%ROWTYPE;
  provider_generation_digest TEXT;
  binding_generation_digest TEXT;
  provider_thumbprint TEXT;
  binding_thumbprint TEXT;
  observed_at TIMESTAMPTZ := clock_timestamp();
BEGIN
  PERFORM okg.external_action_verifier_capability_status();
  IF p_deployment IS NULL OR p_deployment <> bound_deployment
     OR p_authentication_audit_id IS NULL
     OR p_nomos_decision_id IS NULL
     OR p_expected_transition_digest IS NULL
     OR p_expected_transition_digest !~ '^sha256:[0-9a-f]{64}$' THEN
    RAISE EXCEPTION 'external action authority is not deployment-bound'
      USING ERRCODE = '28000';
  END IF;
  PERFORM pg_advisory_xact_lock(
    hashtextextended('okg.external_action.deployment:' || bound_deployment, 0)
  );
  SELECT * INTO audit_row
   FROM okg.external_action_auth_audits
   WHERE deployment_name = bound_deployment
     AND authentication_audit_id = p_authentication_audit_id;
  SELECT * INTO decision_row
   FROM okg.nomos_decisions
   WHERE deployment_name = bound_deployment
     AND decision_id = p_nomos_decision_id;
  SELECT * INTO login_binding_row
    FROM okg.external_action_api_role_bindings
   WHERE "session_user" = session_user
     AND deployment_name = bound_deployment
     AND status = 'active';
  IF audit_row.authentication_audit_id IS NULL
     OR decision_row.decision_id IS NULL
     OR decision_row.effect <> 'allow'
     OR decision_row.subject IS DISTINCT FROM audit_row.separation_subject
     OR (p_expected_action <> '' AND decision_row.action <> p_expected_action)
     OR audit_row.verifier_transition_digest IS DISTINCT FROM p_expected_transition_digest
     OR audit_row.verifier_assertion_expires_at <= observed_at
     OR audit_row.verifier_policy_input_digest IS DISTINCT FROM decision_row.input_hash
     OR audit_row.verifier_capability_generation IS DISTINCT FROM
          login_binding_row.verifier_capability_generation
     OR audit_row.verifier_required_obligations_digest IS DISTINCT FROM
          okg.external_action_hash(
            'okg-external-action-required-obligations-v1',
            COALESCE(decision_row.obligations, '[]'::jsonb)
          )
     OR audit_row.verifier_completed_obligations_digest IS DISTINCT FROM
          okg.external_action_hash(
            'okg-external-action-completed-obligations-v1',
            COALESCE(decision_row.obligations, '[]'::jsonb)
          )
     OR audit_row.verifier_missing_obligations_digest IS DISTINCT FROM
          okg.external_action_hash(
            'okg-external-action-missing-obligations-v1', '[]'::jsonb
          ) THEN
    RAISE EXCEPTION 'Nomos authority is unavailable or stale'
      USING ERRCODE = '28000';
  END IF;
  IF audit_row.authentication_method_code NOT IN ('jwt','jwt_dpop','mtls','trusted_proxy') THEN
    RAISE EXCEPTION 'authentication audit method is not supported by the external-action boundary'
      USING ERRCODE = '28000';
  END IF;
  SELECT * INTO provider_row
    FROM okg.nomos_identity_providers
   WHERE deployment_name = bound_deployment
     AND provider_id = audit_row.nomos_provider_id
     AND status = 'active'
     AND (effective_from IS NULL OR effective_from <= observed_at)
     AND (effective_until IS NULL OR observed_at < effective_until);
  SELECT * INTO binding_row
    FROM okg.nomos_subject_bindings
   WHERE deployment_name = bound_deployment
     AND binding_id = audit_row.nomos_subject_binding_id
     AND provider_id = audit_row.nomos_provider_id
     AND issuer = provider_row.issuer
     AND subject = audit_row.separation_subject
     AND status = 'active'
     AND (effective_from IS NULL OR effective_from <= observed_at)
     AND (effective_until IS NULL OR observed_at < effective_until);
  provider_generation_digest := COALESCE(
    provider_row.metadata ->> 'credential_generation_digest',
    provider_row.metadata ->> 'provider_credential_generation_digest',
    provider_row.metadata ->> 'credential_generation'
  );
  binding_generation_digest := COALESCE(
    binding_row.metadata ->> 'credential_generation_digest',
    binding_row.metadata ->> 'binding_credential_generation_digest',
    binding_row.metadata ->> 'credential_generation'
  );
  provider_thumbprint := COALESCE(
    provider_row.metadata ->> 'certificate_thumbprint',
    provider_row.metadata ->> 'sender_key_thumbprint'
  );
  binding_thumbprint := COALESCE(
    binding_row.metadata ->> 'certificate_thumbprint',
    binding_row.metadata ->> 'sender_key_thumbprint'
  );
  IF provider_row.provider_id IS NULL OR binding_row.binding_id IS NULL
     OR audit_row.nomos_decision_id IS DISTINCT FROM decision_row.decision_id
     OR (provider_generation_digest IS NOT NULL
         AND provider_generation_digest !~ '^sha256:[0-9a-f]{64}$')
     OR (binding_generation_digest IS NOT NULL
         AND binding_generation_digest !~ '^sha256:[0-9a-f]{64}$')
     OR (provider_thumbprint IS NOT NULL
         AND (btrim(provider_thumbprint) <> provider_thumbprint
              OR octet_length(provider_thumbprint) NOT BETWEEN 1 AND 512))
     OR (binding_thumbprint IS NOT NULL
         AND (btrim(binding_thumbprint) <> binding_thumbprint
              OR octet_length(binding_thumbprint) NOT BETWEEN 1 AND 512))
     OR audit_row.provider_status IS DISTINCT FROM provider_row.status
     OR audit_row.binding_status IS DISTINCT FROM binding_row.status
     OR audit_row.provider_updated_at IS DISTINCT FROM provider_row.updated_at
     OR audit_row.binding_updated_at IS DISTINCT FROM binding_row.updated_at
     OR (provider_generation_digest IS NOT NULL
         AND audit_row.provider_credential_generation_digest IS DISTINCT FROM provider_generation_digest)
     OR (binding_generation_digest IS NOT NULL
         AND audit_row.binding_credential_generation_digest IS DISTINCT FROM binding_generation_digest)
     OR (audit_row.credential_generation_digest IS NOT NULL
         AND ((provider_generation_digest IS NOT NULL
               AND audit_row.credential_generation_digest IS DISTINCT FROM provider_generation_digest)
              OR (binding_generation_digest IS NOT NULL
                  AND audit_row.credential_generation_digest IS DISTINCT FROM binding_generation_digest)))
     OR (audit_row.presented_credential_generation_digest IS NOT NULL
         AND ((provider_generation_digest IS NOT NULL
               AND audit_row.presented_credential_generation_digest IS DISTINCT FROM provider_generation_digest)
              OR (binding_generation_digest IS NOT NULL
                  AND audit_row.presented_credential_generation_digest IS DISTINCT FROM binding_generation_digest)))
     OR (audit_row.certificate_thumbprint IS NOT NULL
         AND provider_thumbprint IS NOT NULL
         AND audit_row.certificate_thumbprint IS DISTINCT FROM provider_thumbprint)
     OR (audit_row.certificate_thumbprint IS NOT NULL
         AND binding_thumbprint IS NOT NULL
         AND audit_row.certificate_thumbprint IS DISTINCT FROM binding_thumbprint)
     OR (audit_row.token_issued_at IS NOT NULL
         AND (audit_row.token_issued_at < provider_row.updated_at
              OR audit_row.token_issued_at < binding_row.updated_at))
     OR (audit_row.certificate_not_before IS NOT NULL
         AND (audit_row.certificate_not_before < provider_row.updated_at
              OR audit_row.certificate_not_before < binding_row.updated_at))
     OR audit_row.authentication_method_code IN ('jwt','jwt_dpop','mtls','trusted_proxy')
        AND (audit_row.provider_status IS NULL
             OR audit_row.binding_status IS NULL
             OR audit_row.provider_updated_at IS NULL
             OR audit_row.binding_updated_at IS NULL
             OR audit_row.provider_credential_generation_digest IS NULL
             OR audit_row.binding_credential_generation_digest IS NULL)
     OR audit_row.authentication_method_code IN ('jwt','jwt_dpop')
        AND audit_row.token_issued_at IS NULL
     OR audit_row.authentication_method_code = 'mtls'
        AND (audit_row.certificate_thumbprint IS NULL
             OR audit_row.certificate_not_before IS NULL
             OR audit_row.presented_credential_generation_digest IS NULL) THEN
    RAISE EXCEPTION 'Nomos provider, subject binding, or audit reference is stale'
      USING ERRCODE = '28000';
  END IF;
  RETURN jsonb_build_object(
    'deployment', bound_deployment,
    'authentication_audit_id', audit_row.authentication_audit_id,
    'nomos_decision_id', decision_row.decision_id,
    'nomos_provider_id', provider_row.provider_id,
    'nomos_subject_binding_id', binding_row.binding_id,
    'principal', audit_row.principal,
    'separation_subject', audit_row.separation_subject,
    'provider_status', provider_row.status,
    'provider_updated_at', provider_row.updated_at,
    'binding_status', binding_row.status,
    'binding_updated_at', binding_row.updated_at,
    'provider_credential_generation_digest', provider_generation_digest,
    'binding_credential_generation_digest', binding_generation_digest,
    'credential_generation_digest', audit_row.credential_generation_digest,
    'presented_credential_generation_digest', audit_row.presented_credential_generation_digest,
    'token_issued_at', audit_row.token_issued_at,
    'certificate_thumbprint', audit_row.certificate_thumbprint,
    'certificate_not_before', audit_row.certificate_not_before,
    'certificate_not_after', audit_row.certificate_not_after,
    'policy_digest', decision_row.policy_fingerprint,
    'action', decision_row.action,
    'effect', decision_row.effect,
    'reason_code', decision_row.reason_code
    ,'verifier_transition_digest', audit_row.verifier_transition_digest
    ,'verifier_capability_generation', audit_row.verifier_capability_generation
  );
END
$external_action_nomos_authority_snapshot$;

CREATE OR REPLACE FUNCTION okg.external_action_record_auth_audit(p_command JSONB)
RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $external_action_record_auth_audit$
DECLARE
  bound_deployment TEXT := okg.external_action_session_deployment();
  audit_row okg.external_action_auth_audits%ROWTYPE;
  provider_row okg.nomos_identity_providers%ROWTYPE;
  binding_row okg.nomos_subject_bindings%ROWTYPE;
  provider_generation_digest TEXT;
  binding_generation_digest TEXT;
  provider_thumbprint TEXT;
  binding_thumbprint TEXT;
  decision_row okg.nomos_decisions%ROWTYPE;
  credential_evidence_digest TEXT;
  verifier_assertion JSONB;
  digest_key TEXT;
  timestamp_key TEXT;
BEGIN
  PERFORM okg.external_action_require_command(
    p_command,
    ARRAY[
      'deployment','authentication_audit_id','request_id','principal',
      'separation_subject','nomos_provider_id','nomos_subject_binding_id',
      'authentication_method_code','issuer_or_trust_domain_digest',
      'sender_key_or_certificate_thumbprint','token_or_certificate_id_digest',
      'dpop_jti_digest','auth_config_digest','result_code','observed_at',
      'credential_expires_at','provider_status','binding_status',
      'provider_updated_at','binding_updated_at',
      'provider_credential_generation_digest','binding_credential_generation_digest',
      'credential_generation_digest','presented_credential_generation_digest',
      'token_issued_at','certificate_thumbprint','certificate_not_before',
      'certificate_not_after',
      'nomos_decision_id','verifier_assertion','verifier_assertion_tag'
    ],
    ARRAY[
      'deployment','authentication_audit_id','request_id','principal',
      'separation_subject','nomos_provider_id','nomos_subject_binding_id',
      'authentication_method_code','issuer_or_trust_domain_digest',
      'auth_config_digest','result_code'
      ,'nomos_decision_id','verifier_assertion','verifier_assertion_tag'
    ]
  );
  IF p_command ->> 'deployment' <> bound_deployment THEN
    RAISE EXCEPTION 'external action deployment is not bound to session_user'
      USING ERRCODE = '28000';
  END IF;
  IF p_command ->> 'authentication_method_code' NOT IN ('jwt','jwt_dpop','mtls','trusted_proxy') THEN
    RAISE EXCEPTION 'authentication audit method is not supported by the external-action boundary'
      USING ERRCODE = '22023';
  END IF;
  -- The submitted identity projection must describe one current database
  -- provider/binding pair.  This closes mixed-provider and stale-generation
  -- projections before they become durable audit evidence.
  SELECT * INTO provider_row
    FROM okg.nomos_identity_providers
   WHERE deployment_name = bound_deployment
     AND provider_id = p_command ->> 'nomos_provider_id';
  SELECT * INTO binding_row
    FROM okg.nomos_subject_bindings
   WHERE deployment_name = bound_deployment
     AND binding_id = p_command ->> 'nomos_subject_binding_id'
     AND provider_id = p_command ->> 'nomos_provider_id'
     AND issuer = provider_row.issuer
     AND subject = p_command ->> 'separation_subject';
  provider_generation_digest := COALESCE(
    provider_row.metadata ->> 'credential_generation_digest',
    provider_row.metadata ->> 'provider_credential_generation_digest',
    provider_row.metadata ->> 'credential_generation'
  );
  binding_generation_digest := COALESCE(
    binding_row.metadata ->> 'credential_generation_digest',
    binding_row.metadata ->> 'binding_credential_generation_digest',
    binding_row.metadata ->> 'credential_generation'
  );
  provider_thumbprint := COALESCE(
    provider_row.metadata ->> 'certificate_thumbprint',
    provider_row.metadata ->> 'sender_key_thumbprint'
  );
  binding_thumbprint := COALESCE(
    binding_row.metadata ->> 'certificate_thumbprint',
    binding_row.metadata ->> 'sender_key_thumbprint'
  );
  IF provider_row.provider_id IS NULL
     OR binding_row.binding_id IS NULL
     OR provider_row.status <> 'active'
     OR binding_row.status <> 'active'
     OR (provider_row.effective_from IS NOT NULL AND provider_row.effective_from > clock_timestamp())
     OR (provider_row.effective_until IS NOT NULL AND clock_timestamp() >= provider_row.effective_until)
     OR (binding_row.effective_from IS NOT NULL AND binding_row.effective_from > clock_timestamp())
     OR (binding_row.effective_until IS NOT NULL AND clock_timestamp() >= binding_row.effective_until)
     OR p_command ->> 'principal' IS DISTINCT FROM binding_row.subject
     OR p_command ->> 'separation_subject' IS DISTINCT FROM binding_row.subject
     OR p_command ->> 'issuer_or_trust_domain_digest' IS DISTINCT FROM
          'sha256:' || encode(
            sha256(convert_to(provider_row.issuer, 'UTF8')), 'hex')
     OR p_command ->> 'provider_status' IS DISTINCT FROM provider_row.status
     OR p_command ->> 'binding_status' IS DISTINCT FROM binding_row.status
     OR (provider_generation_digest IS NULL
         AND p_command ->> 'provider_credential_generation_digest' IS NOT NULL)
     OR (provider_generation_digest IS NOT NULL
         AND p_command ->> 'provider_credential_generation_digest' IS DISTINCT FROM provider_generation_digest)
     OR (binding_generation_digest IS NULL
         AND p_command ->> 'binding_credential_generation_digest' IS NOT NULL)
     OR (binding_generation_digest IS NOT NULL
         AND p_command ->> 'binding_credential_generation_digest' IS DISTINCT FROM binding_generation_digest)
     OR (provider_thumbprint IS NOT NULL
         AND p_command ->> 'certificate_thumbprint' IS NOT NULL
         AND p_command ->> 'certificate_thumbprint' IS DISTINCT FROM provider_thumbprint)
     OR (binding_thumbprint IS NOT NULL
         AND p_command ->> 'certificate_thumbprint' IS NOT NULL
         AND p_command ->> 'certificate_thumbprint' IS DISTINCT FROM binding_thumbprint)
     OR p_command ->> 'provider_updated_at' IS DISTINCT FROM to_char(provider_row.updated_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
     OR p_command ->> 'binding_updated_at' IS DISTINCT FROM to_char(binding_row.updated_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"') THEN
    RAISE EXCEPTION 'authentication audit identity is not a live database provider binding'
      USING ERRCODE = '28000';
  END IF;
  -- JSONB ->> coerces numbers and booleans to text.  Every retained evidence
  -- field therefore checks its JSON type before the cast below; otherwise a
  -- caller could smuggle a non-canonical timestamp or digest into an audit.
  FOREACH digest_key IN ARRAY ARRAY[
    'issuer_or_trust_domain_digest','token_or_certificate_id_digest',
    'dpop_jti_digest','auth_config_digest',
    'provider_credential_generation_digest','binding_credential_generation_digest',
    'credential_generation_digest','presented_credential_generation_digest'
  ] LOOP
    IF p_command ? digest_key AND p_command -> digest_key <> 'null'::jsonb THEN
      IF jsonb_typeof(p_command -> digest_key) IS DISTINCT FROM 'string'
         OR p_command ->> digest_key !~ '^sha256:[0-9a-f]{64}$' THEN
        RAISE EXCEPTION 'authentication audit digest is not canonical'
          USING ERRCODE = '22023';
      END IF;
    END IF;
  END LOOP;
  FOREACH timestamp_key IN ARRAY ARRAY[
    'observed_at','credential_expires_at','provider_updated_at',
    'binding_updated_at','token_issued_at','certificate_not_before',
    'certificate_not_after'
  ] LOOP
    IF p_command ? timestamp_key AND p_command -> timestamp_key <> 'null'::jsonb THEN
      IF jsonb_typeof(p_command -> timestamp_key) IS DISTINCT FROM 'string'
         OR p_command ->> timestamp_key !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[.][0-9]{6}Z$' THEN
        RAISE EXCEPTION 'authentication audit timestamp is not canonical'
          USING ERRCODE = '22023';
      END IF;
      -- Reject impossible calendar values (the regex only establishes the
      -- wire shape; PostgreSQL performs the calendar validation here).
      PERFORM (p_command ->> timestamp_key)::timestamptz;
    END IF;
  END LOOP;
  FOREACH timestamp_key IN ARRAY ARRAY['provider_status','binding_status'] LOOP
    IF p_command ? timestamp_key AND p_command -> timestamp_key <> 'null'::jsonb
       AND (jsonb_typeof(p_command -> timestamp_key) IS DISTINCT FROM 'string'
            OR btrim(p_command ->> timestamp_key) = '') THEN
      RAISE EXCEPTION 'authentication audit status is not canonical'
        USING ERRCODE = '22023';
    END IF;
  END LOOP;
  IF p_command ? 'provider_status' AND p_command ->> 'provider_status' IS NOT NULL
     AND p_command ->> 'provider_status' NOT IN ('active','disabled','rotating','revoked') THEN
    RAISE EXCEPTION 'authentication audit provider status is not canonical' USING ERRCODE = '22023';
  END IF;
  IF p_command ? 'binding_status' AND p_command ->> 'binding_status' IS NOT NULL
     AND p_command ->> 'binding_status' NOT IN ('active','disabled') THEN
    RAISE EXCEPTION 'authentication audit binding status is not canonical' USING ERRCODE = '22023';
  END IF;
  IF p_command ? 'certificate_thumbprint' AND p_command -> 'certificate_thumbprint' <> 'null'::jsonb
     AND (jsonb_typeof(p_command -> 'certificate_thumbprint') IS DISTINCT FROM 'string'
          OR btrim(p_command ->> 'certificate_thumbprint') <> p_command ->> 'certificate_thumbprint'
          OR octet_length(p_command ->> 'certificate_thumbprint') NOT BETWEEN 1 AND 512) THEN
    RAISE EXCEPTION 'authentication audit certificate thumbprint is not canonical' USING ERRCODE = '22023';
  END IF;
  IF p_command ->> 'certificate_not_before' IS NOT NULL
     AND p_command ->> 'certificate_not_after' IS NOT NULL
     AND (p_command ->> 'certificate_not_after')::timestamptz
         < (p_command ->> 'certificate_not_before')::timestamptz THEN
    RAISE EXCEPTION 'authentication audit certificate interval is not canonical' USING ERRCODE = '22023';
  END IF;
  IF p_command ? 'sender_key_or_certificate_thumbprint'
     AND p_command ? 'certificate_thumbprint'
     AND p_command -> 'sender_key_or_certificate_thumbprint' <> 'null'::jsonb
     AND p_command -> 'certificate_thumbprint' <> 'null'::jsonb
     AND p_command ->> 'sender_key_or_certificate_thumbprint' IS DISTINCT FROM p_command ->> 'certificate_thumbprint' THEN
    RAISE EXCEPTION 'authentication audit certificate thumbprint disagrees with sender constraint' USING ERRCODE = '22023';
  END IF;
  IF p_command ->> 'authentication_method_code' IN ('jwt','jwt_dpop','mtls','trusted_proxy')
     AND (p_command ->> 'provider_status' IS NULL
          OR p_command ->> 'binding_status' IS NULL
          OR p_command ->> 'provider_updated_at' IS NULL
          OR p_command ->> 'binding_updated_at' IS NULL
          OR p_command ->> 'provider_credential_generation_digest' IS NULL
          OR p_command ->> 'binding_credential_generation_digest' IS NULL) THEN
    RAISE EXCEPTION 'authentication audit identity barrier is incomplete' USING ERRCODE = '28000';
  END IF;
  IF p_command ->> 'authentication_method_code' IN ('jwt','jwt_dpop')
     AND p_command ->> 'token_issued_at' IS NULL THEN
    RAISE EXCEPTION 'authentication audit JWT issuance evidence is incomplete' USING ERRCODE = '28000';
  END IF;
  IF p_command ->> 'authentication_method_code' = 'mtls'
     AND (p_command ->> 'certificate_thumbprint' IS NULL
          OR p_command ->> 'certificate_not_before' IS NULL
          OR p_command ->> 'presented_credential_generation_digest' IS NULL) THEN
    RAISE EXCEPTION 'authentication audit certificate evidence is incomplete' USING ERRCODE = '28000';
  END IF;
  IF p_command ->> 'nomos_decision_id' IS NOT NULL THEN
    SELECT d.* INTO decision_row
      FROM okg.nomos_decisions d
     WHERE d.deployment_name = bound_deployment
       AND d.decision_id = p_command ->> 'nomos_decision_id';
    IF decision_row.decision_id IS NULL
       OR decision_row.subject IS DISTINCT FROM p_command ->> 'separation_subject' THEN
      RAISE EXCEPTION 'Nomos decision subject is not the audit separation subject'
        USING ERRCODE = '42501';
    END IF;
  END IF;
  credential_evidence_digest := okg.external_action_hash(
    'okg-external-action-credential-evidence-v1',
    jsonb_build_object(
      'principal', p_command -> 'principal',
      'separation_subject', p_command -> 'separation_subject',
      'nomos_provider_id', p_command -> 'nomos_provider_id',
      'nomos_subject_binding_id', p_command -> 'nomos_subject_binding_id',
      'authentication_method_code', p_command -> 'authentication_method_code',
      'issuer_or_trust_domain_digest', p_command -> 'issuer_or_trust_domain_digest',
      'sender_key_or_certificate_thumbprint', p_command -> 'sender_key_or_certificate_thumbprint',
      'token_or_certificate_id_digest', p_command -> 'token_or_certificate_id_digest',
      'dpop_jti_digest', p_command -> 'dpop_jti_digest',
      'auth_config_digest', p_command -> 'auth_config_digest',
      'result_code', p_command -> 'result_code',
      'credential_expires_at', p_command -> 'credential_expires_at',
      'provider_status', p_command -> 'provider_status',
      'binding_status', p_command -> 'binding_status',
      'provider_updated_at', p_command -> 'provider_updated_at',
      'binding_updated_at', p_command -> 'binding_updated_at',
      'provider_credential_generation_digest', p_command -> 'provider_credential_generation_digest',
      'binding_credential_generation_digest', p_command -> 'binding_credential_generation_digest',
      'credential_generation_digest', p_command -> 'credential_generation_digest',
      'presented_credential_generation_digest', p_command -> 'presented_credential_generation_digest',
      'token_issued_at', p_command -> 'token_issued_at',
      'certificate_thumbprint', p_command -> 'certificate_thumbprint',
      'certificate_not_before', p_command -> 'certificate_not_before',
      'certificate_not_after', p_command -> 'certificate_not_after'
    )
  );
  verifier_assertion := okg.external_action_verify_verifier_assertion(
    p_command -> 'verifier_assertion',
    p_command ->> 'verifier_assertion_tag',
    jsonb_build_object(
      'deployment', bound_deployment,
      'authentication_audit_id', p_command ->> 'authentication_audit_id',
      'credential_evidence_digest', credential_evidence_digest,
      'nomos_decision_id', decision_row.decision_id,
      'policy_fingerprint', decision_row.policy_fingerprint,
      'provider_id', provider_row.provider_id,
      'provider_status', provider_row.status,
      'provider_updated_at', p_command ->> 'provider_updated_at',
      'binding_id', binding_row.binding_id,
      'binding_status', binding_row.status,
      'binding_updated_at', p_command ->> 'binding_updated_at',
      'nomos_input_hash', decision_row.input_hash,
      'allowed', true,
      'final_effect', 'allow'
    )
  );
  PERFORM pg_advisory_xact_lock(
    hashtextextended('okg.external_action.deployment:' || bound_deployment, 0)
  );
  INSERT INTO okg.external_action_auth_audits (
    authentication_audit_id, deployment_name, request_id, principal,
    separation_subject, nomos_provider_id, nomos_subject_binding_id,
    authentication_method_code, issuer_or_trust_domain_digest,
    sender_key_or_certificate_thumbprint, token_or_certificate_id_digest,
    dpop_jti_digest, auth_config_digest, result_code, observed_at,
    credential_expires_at, provider_status, binding_status,
    provider_updated_at, binding_updated_at,
    provider_credential_generation_digest, binding_credential_generation_digest,
    credential_generation_digest, presented_credential_generation_digest,
    token_issued_at, certificate_thumbprint, certificate_not_before,
    certificate_not_after,
    nomos_decision_id,
    verifier_assertion_nonce_digest, verifier_assertion_expires_at,
    verifier_transition_digest, verifier_policy_input_digest,
    verifier_required_obligations_digest,
    verifier_completed_obligations_digest,
    verifier_missing_obligations_digest, verifier_assertion_tag,
    verifier_capability_generation, store_record
  ) VALUES (
    p_command ->> 'authentication_audit_id', bound_deployment,
    p_command ->> 'request_id', p_command ->> 'principal',
    p_command ->> 'separation_subject', p_command ->> 'nomos_provider_id',
    p_command ->> 'nomos_subject_binding_id',
    p_command ->> 'authentication_method_code',
    p_command ->> 'issuer_or_trust_domain_digest',
    p_command ->> 'sender_key_or_certificate_thumbprint',
    p_command ->> 'token_or_certificate_id_digest',
    p_command ->> 'dpop_jti_digest', p_command ->> 'auth_config_digest',
    p_command ->> 'result_code',
    COALESCE((p_command ->> 'observed_at')::timestamptz, clock_timestamp()),
    NULLIF(p_command ->> 'credential_expires_at', '')::timestamptz,
    p_command ->> 'provider_status', p_command ->> 'binding_status',
    NULLIF(p_command ->> 'provider_updated_at', '')::timestamptz,
    NULLIF(p_command ->> 'binding_updated_at', '')::timestamptz,
    p_command ->> 'provider_credential_generation_digest',
    p_command ->> 'binding_credential_generation_digest',
    p_command ->> 'credential_generation_digest',
    p_command ->> 'presented_credential_generation_digest',
    NULLIF(p_command ->> 'token_issued_at', '')::timestamptz,
    p_command ->> 'certificate_thumbprint',
    NULLIF(p_command ->> 'certificate_not_before', '')::timestamptz,
    NULLIF(p_command ->> 'certificate_not_after', '')::timestamptz,
    p_command ->> 'nomos_decision_id',
    verifier_assertion ->> 'nonce_digest',
    (verifier_assertion ->> 'expires_at')::timestamptz,
    verifier_assertion ->> 'transition_digest',
    verifier_assertion ->> 'nomos_input_hash',
    verifier_assertion ->> 'required_obligations_digest',
    verifier_assertion ->> 'completed_obligations_digest',
    verifier_assertion ->> 'missing_obligations_digest',
    p_command ->> 'verifier_assertion_tag',
    (verifier_assertion ->> 'capability_generation')::bigint,
    p_command
  )
  ON CONFLICT (deployment_name, authentication_audit_id) DO NOTHING;
  SELECT * INTO audit_row
    FROM okg.external_action_auth_audits
   WHERE deployment_name = bound_deployment
     AND authentication_audit_id = p_command ->> 'authentication_audit_id';
  IF audit_row.authentication_audit_id IS NULL THEN
    RAISE EXCEPTION 'authentication audit was not stored'
      USING ERRCODE = '40001';
  END IF;
  IF audit_row.request_id <> p_command ->> 'request_id'
     OR audit_row.principal <> p_command ->> 'principal'
     OR audit_row.separation_subject <> p_command ->> 'separation_subject'
     OR audit_row.auth_config_digest <> p_command ->> 'auth_config_digest'
     OR audit_row.nomos_provider_id <> p_command ->> 'nomos_provider_id'
     OR audit_row.nomos_subject_binding_id <> p_command ->> 'nomos_subject_binding_id'
     OR audit_row.authentication_method_code <> p_command ->> 'authentication_method_code'
     OR audit_row.issuer_or_trust_domain_digest <> p_command ->> 'issuer_or_trust_domain_digest'
     OR audit_row.sender_key_or_certificate_thumbprint IS DISTINCT FROM p_command ->> 'sender_key_or_certificate_thumbprint'
     OR audit_row.token_or_certificate_id_digest IS DISTINCT FROM p_command ->> 'token_or_certificate_id_digest'
     OR audit_row.dpop_jti_digest IS DISTINCT FROM p_command ->> 'dpop_jti_digest'
     OR audit_row.result_code <> p_command ->> 'result_code'
     OR audit_row.credential_expires_at IS DISTINCT FROM NULLIF(p_command ->> 'credential_expires_at', '')::timestamptz
     OR audit_row.provider_status IS DISTINCT FROM p_command ->> 'provider_status'
     OR audit_row.binding_status IS DISTINCT FROM p_command ->> 'binding_status'
     OR audit_row.provider_updated_at IS DISTINCT FROM NULLIF(p_command ->> 'provider_updated_at', '')::timestamptz
     OR audit_row.binding_updated_at IS DISTINCT FROM NULLIF(p_command ->> 'binding_updated_at', '')::timestamptz
     OR audit_row.provider_credential_generation_digest IS DISTINCT FROM p_command ->> 'provider_credential_generation_digest'
     OR audit_row.binding_credential_generation_digest IS DISTINCT FROM p_command ->> 'binding_credential_generation_digest'
     OR audit_row.credential_generation_digest IS DISTINCT FROM p_command ->> 'credential_generation_digest'
     OR audit_row.presented_credential_generation_digest IS DISTINCT FROM p_command ->> 'presented_credential_generation_digest'
     OR audit_row.token_issued_at IS DISTINCT FROM NULLIF(p_command ->> 'token_issued_at', '')::timestamptz
     OR audit_row.certificate_thumbprint IS DISTINCT FROM p_command ->> 'certificate_thumbprint'
     OR audit_row.certificate_not_before IS DISTINCT FROM NULLIF(p_command ->> 'certificate_not_before', '')::timestamptz
     OR audit_row.certificate_not_after IS DISTINCT FROM NULLIF(p_command ->> 'certificate_not_after', '')::timestamptz
     OR audit_row.nomos_decision_id IS DISTINCT FROM p_command ->> 'nomos_decision_id'
     OR audit_row.verifier_assertion_nonce_digest IS DISTINCT FROM verifier_assertion ->> 'nonce_digest'
     OR audit_row.verifier_assertion_tag IS DISTINCT FROM p_command ->> 'verifier_assertion_tag'
     OR audit_row.store_record IS DISTINCT FROM p_command THEN
    RAISE EXCEPTION 'authentication audit id was reused with a different request'
      USING ERRCODE = '40001';
  END IF;
  RETURN jsonb_build_object(
    'replay', FALSE,
    'authentication_audit_id', audit_row.authentication_audit_id,
    'deployment', audit_row.deployment_name,
    'request_id', audit_row.request_id,
    'principal', audit_row.principal,
    'separation_subject', audit_row.separation_subject,
    'nomos_provider_id', audit_row.nomos_provider_id,
    'nomos_subject_binding_id', audit_row.nomos_subject_binding_id,
    'authentication_method_code', audit_row.authentication_method_code,
    'issuer_or_trust_domain_digest', audit_row.issuer_or_trust_domain_digest,
    'sender_key_or_certificate_thumbprint', audit_row.sender_key_or_certificate_thumbprint,
    'token_or_certificate_id_digest', audit_row.token_or_certificate_id_digest,
    'dpop_jti_digest', audit_row.dpop_jti_digest,
    'auth_config_digest', audit_row.auth_config_digest,
    'result_code', audit_row.result_code,
    'observed_at', audit_row.observed_at,
    'credential_expires_at', audit_row.credential_expires_at,
    'provider_status', audit_row.provider_status,
    'binding_status', audit_row.binding_status,
    'provider_updated_at', audit_row.provider_updated_at,
    'binding_updated_at', audit_row.binding_updated_at,
    'provider_credential_generation_digest', audit_row.provider_credential_generation_digest,
    'binding_credential_generation_digest', audit_row.binding_credential_generation_digest,
    'credential_generation_digest', audit_row.credential_generation_digest,
    'presented_credential_generation_digest', audit_row.presented_credential_generation_digest,
    'token_issued_at', audit_row.token_issued_at,
    'certificate_thumbprint', audit_row.certificate_thumbprint,
    'certificate_not_before', audit_row.certificate_not_before,
    'certificate_not_after', audit_row.certificate_not_after,
    'nomos_decision_id', audit_row.nomos_decision_id
  );
END
$external_action_record_auth_audit$;

CREATE OR REPLACE FUNCTION okg.external_action_nomos_policy_bundle(
  p_scope TEXT,
  p_provider_id TEXT,
  p_binding_id TEXT
) RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $external_action_nomos_policy_bundle$
DECLARE
  bound_deployment TEXT := okg.external_action_session_deployment();
  observed_at TIMESTAMPTZ := clock_timestamp();
  activation_count INTEGER;
  activation_row okg.nomos_policy_activations%ROWTYPE;
  bundle_row okg.nomos_policy_bundles%ROWTYPE;
  provider_row okg.nomos_identity_providers%ROWTYPE;
  binding_row okg.nomos_subject_bindings%ROWTYPE;
  provider_generation_digest TEXT;
  binding_generation_digest TEXT;
BEGIN
  PERFORM okg.external_action_verifier_capability_status();
  IF p_scope IS NULL OR btrim(p_scope) = ''
     OR p_provider_id IS NULL OR btrim(p_provider_id) = ''
     OR p_binding_id IS NULL OR btrim(p_binding_id) = '' THEN
    RAISE EXCEPTION 'Nomos policy authority request is incomplete'
      USING ERRCODE = '22023';
  END IF;
  PERFORM pg_advisory_xact_lock(
    hashtextextended(
      'okg.external_action.deployment:' || bound_deployment, 0
    )
  );
  SELECT count(*)::INTEGER INTO activation_count
    FROM okg.nomos_policy_activations a
    JOIN okg.nomos_policy_bundles b
      ON b.deployment_name = a.deployment_name
     AND b.policy_fingerprint = a.policy_fingerprint
   WHERE a.deployment_name = bound_deployment
     AND a.scope = p_scope
     AND a.status = 'active'
     AND b.status = 'active'
     AND (b.effective_from IS NULL OR b.effective_from <= observed_at)
     AND (b.effective_until IS NULL OR observed_at < b.effective_until);
  IF activation_count <> 1 THEN
    RAISE EXCEPTION 'active Nomos policy bundle is unavailable'
      USING ERRCODE = '42501';
  END IF;
  SELECT a.* INTO activation_row
    FROM okg.nomos_policy_activations a
   WHERE a.deployment_name = bound_deployment
     AND a.scope = p_scope
     AND a.status = 'active';
  SELECT b.* INTO bundle_row
    FROM okg.nomos_policy_bundles b
   WHERE b.deployment_name = bound_deployment
     AND b.policy_fingerprint = activation_row.policy_fingerprint
     AND b.status = 'active'
     AND (b.effective_from IS NULL OR b.effective_from <= observed_at)
     AND (b.effective_until IS NULL OR observed_at < b.effective_until);
  SELECT p.* INTO provider_row
    FROM okg.nomos_identity_providers p
   WHERE p.deployment_name = bound_deployment
     AND p.provider_id = p_provider_id;
  SELECT s.* INTO binding_row
    FROM okg.nomos_subject_bindings s
   WHERE s.deployment_name = bound_deployment
     AND s.binding_id = p_binding_id
     AND s.provider_id = p_provider_id
     AND s.issuer = provider_row.issuer
     AND s.scope = p_scope;
  IF bundle_row.policy_fingerprint IS NULL
     OR provider_row.provider_id IS NULL
     OR binding_row.binding_id IS NULL
     OR provider_row.status <> 'active'
     OR binding_row.status <> 'active'
     OR (provider_row.effective_from IS NOT NULL
         AND provider_row.effective_from > observed_at)
     OR (provider_row.effective_until IS NOT NULL
         AND observed_at >= provider_row.effective_until)
     OR (binding_row.effective_from IS NOT NULL
         AND binding_row.effective_from > observed_at)
     OR (binding_row.effective_until IS NOT NULL
         AND observed_at >= binding_row.effective_until) THEN
    RAISE EXCEPTION 'active Nomos provider or subject binding is unavailable'
      USING ERRCODE = '42501';
  END IF;
  provider_generation_digest := COALESCE(
    provider_row.metadata ->> 'credential_generation_digest',
    provider_row.metadata ->> 'provider_credential_generation_digest',
    provider_row.metadata ->> 'credential_generation'
  );
  binding_generation_digest := COALESCE(
    binding_row.metadata ->> 'credential_generation_digest',
    binding_row.metadata ->> 'binding_credential_generation_digest',
    binding_row.metadata ->> 'credential_generation'
  );
  RETURN jsonb_build_object(
    'deployment', bound_deployment,
    'scope', activation_row.scope,
    'activation_id', activation_row.activation_id,
    'policy_bundle_id', bundle_row.policy_bundle_id,
    'policy_version', bundle_row.policy_version,
    'policy_fingerprint', bundle_row.policy_fingerprint,
    'policy_bundle_updated_at', bundle_row.updated_at,
    'bundle', bundle_row.bundle,
    'provider_id', provider_row.provider_id,
    'provider_status', provider_row.status,
    'provider_updated_at', provider_row.updated_at,
    'provider_credential_generation_digest', provider_generation_digest,
    'provider_certificate_thumbprint', COALESCE(
      provider_row.metadata ->> 'certificate_thumbprint',
      provider_row.metadata ->> 'sender_key_thumbprint'
    ),
    'binding_id', binding_row.binding_id,
    'subject', binding_row.subject,
    'binding_status', binding_row.status,
    'binding_updated_at', binding_row.updated_at,
    'binding_credential_generation_digest', binding_generation_digest,
    'binding_certificate_thumbprint', COALESCE(
      binding_row.metadata ->> 'certificate_thumbprint',
      binding_row.metadata ->> 'sender_key_thumbprint'
    ),
    'authority_barrier', jsonb_build_object(
      'policy_fingerprint', bundle_row.policy_fingerprint,
      'policy_bundle_updated_at', bundle_row.updated_at,
      'provider_id', provider_row.provider_id,
      'provider_status', provider_row.status,
      'provider_updated_at', provider_row.updated_at,
      'provider_credential_generation_digest', provider_generation_digest,
      'binding_id', binding_row.binding_id,
      'binding_status', binding_row.status,
      'binding_updated_at', binding_row.updated_at,
      'binding_credential_generation_digest', binding_generation_digest
    )
  );
END
$external_action_nomos_policy_bundle$;

CREATE OR REPLACE FUNCTION okg.external_action_nomos_persist_decision(
  p_command JSONB
) RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $external_action_nomos_persist_decision$
DECLARE
  bound_deployment TEXT := okg.external_action_session_deployment();
  decision_row okg.nomos_decisions%ROWTYPE;
  authority_row JSONB;
  inserted BOOLEAN;
  inserted_count INTEGER;
  rule JSONB;
  selected_rule JSONB;
  facts JSONB;
  expected JSONB;
  actual JSONB;
  match_key TEXT;
  rule_matches BOOLEAN;
  expected_effect TEXT;
  expected_reason_code TEXT;
  expected_matched_rule_ids JSONB;
  expected_obligations JSONB;
  expected_input_hash TEXT;
  expected_required_obligations_digest TEXT;
  expected_completed_obligations_digest TEXT;
  expected_missing_obligations_digest TEXT;
BEGIN
  PERFORM okg.external_action_require_command(
    p_command,
    ARRAY[
      'deployment','decision_id','policy_bundle_id','policy_version',
      'policy_fingerprint','input_hash','subject','action','resource',
      'classification','context','effect','reason_code','matched_rule_ids',
      'obligations','enforcement_point','target','created_at',
      'policy_scope','provider_id','binding_id','provider_status',
      'provider_updated_at','binding_status','binding_updated_at',
      'verifier_assertion','verifier_assertion_tag','allowed','final_effect',
      'required_obligations','completed_obligations','missing_obligations'
    ],
    ARRAY[
      'deployment','decision_id','input_hash','subject','action',
      'classification','effect','reason_code','enforcement_point',
      'policy_bundle_id','policy_version','policy_fingerprint',
      'policy_scope','provider_id','binding_id','provider_status',
      'provider_updated_at','binding_status','binding_updated_at',
      'verifier_assertion','verifier_assertion_tag','allowed','final_effect',
      'required_obligations','completed_obligations','missing_obligations'
    ]
  );
  IF p_command ->> 'deployment' <> bound_deployment THEN
    RAISE EXCEPTION 'external action deployment is not bound to session_user'
      USING ERRCODE = '28000';
  END IF;
  PERFORM pg_advisory_xact_lock(
    hashtextextended('okg.external_action.deployment:' || bound_deployment, 0)
  );
  authority_row := okg.external_action_nomos_policy_bundle(
    p_command ->> 'policy_scope',
    p_command ->> 'provider_id',
    p_command ->> 'binding_id'
  );
  IF authority_row ->> 'policy_bundle_id' IS DISTINCT FROM
       p_command ->> 'policy_bundle_id'
     OR authority_row ->> 'policy_version' IS DISTINCT FROM
       p_command ->> 'policy_version'
     OR authority_row ->> 'policy_fingerprint' IS DISTINCT FROM
       p_command ->> 'policy_fingerprint'
     OR authority_row ->> 'provider_status' IS DISTINCT FROM
       p_command ->> 'provider_status'
     OR (authority_row ->> 'provider_updated_at')::timestamptz IS DISTINCT FROM
       (p_command ->> 'provider_updated_at')::timestamptz
     OR authority_row ->> 'binding_status' IS DISTINCT FROM
       p_command ->> 'binding_status'
     OR (authority_row ->> 'binding_updated_at')::timestamptz IS DISTINCT FROM
       (p_command ->> 'binding_updated_at')::timestamptz THEN
    RAISE EXCEPTION 'Nomos policy or identity authority changed during decision'
      USING ERRCODE = '40001';
  END IF;
  -- The API supplies a policy decision projection, but that projection is
  -- not an authority.  Re-evaluate the request against the locked active
  -- bundle here, binding subject to the database-owned subject binding.  A
  -- caller can therefore not mint an allow by changing subject, effect, or
  -- reason fields in the JSON command.
  IF authority_row ->> 'subject' IS NULL
     OR jsonb_typeof(authority_row -> 'bundle') IS DISTINCT FROM 'object'
     OR jsonb_typeof(authority_row -> 'bundle' -> 'rules') IS DISTINCT FROM 'array'
     OR jsonb_typeof(p_command -> 'resource') IS DISTINCT FROM 'object'
     OR jsonb_typeof(p_command -> 'context') IS DISTINCT FROM 'object'
     OR jsonb_typeof(p_command -> 'matched_rule_ids') IS DISTINCT FROM 'array'
     OR jsonb_typeof(p_command -> 'obligations') IS DISTINCT FROM 'array' THEN
    RAISE EXCEPTION 'Nomos database authorization input is malformed'
      USING ERRCODE = '22023';
  END IF;
  -- Match Nomos core's fact construction exactly: canonical fields are
  -- seeded first, then resource and context fields overlay them.
  facts := jsonb_build_object(
      'subject', authority_row ->> 'subject',
      'action', p_command ->> 'action',
      'classification', p_command ->> 'classification'
    ) || COALESCE(p_command -> 'resource', '{}'::jsonb)
      || COALESCE(p_command -> 'context', '{}'::jsonb);
  FOR rule IN
    SELECT value
      FROM jsonb_array_elements(authority_row -> 'bundle' -> 'rules') AS entries(value)
     ORDER BY CASE
                WHEN value ->> 'priority' ~ '^-?[0-9]+$'
                  THEN (value ->> 'priority')::integer
                ELSE 0
              END DESC,
              value ->> 'rule_id'
  LOOP
    IF jsonb_typeof(rule) IS DISTINCT FROM 'object'
       OR jsonb_typeof(rule -> 'actions') IS DISTINCT FROM 'array'
       OR jsonb_typeof(rule -> 'subjects') IS DISTINCT FROM 'array'
       OR (rule ? 'match' AND jsonb_typeof(rule -> 'match') IS DISTINCT FROM 'object')
       OR rule ->> 'rule_id' IS NULL
       OR rule ->> 'effect' IS NULL THEN
      RAISE EXCEPTION 'Nomos active policy rule is malformed'
        USING ERRCODE = '42501';
    END IF;
    IF (rule -> 'actions') ? '*'
       OR (rule -> 'actions') ? (p_command ->> 'action') THEN
      IF (rule -> 'subjects') ? '*'
         OR (rule -> 'subjects') ? (authority_row ->> 'subject') THEN
        rule_matches := TRUE;
        FOR match_key, expected IN
          SELECT key, value
            FROM jsonb_each(COALESCE(rule -> 'match', '{}'::jsonb))
        LOOP
          IF NOT facts ? match_key THEN
            rule_matches := FALSE;
          ELSE
            actual := facts -> match_key;
            IF jsonb_typeof(expected) = 'array' THEN
              IF jsonb_typeof(actual) = 'array' THEN
                IF NOT (expected @> actual) THEN
                  rule_matches := FALSE;
                END IF;
              ELSIF NOT (expected @> jsonb_build_array(actual)) THEN
                rule_matches := FALSE;
              END IF;
            ELSIF jsonb_typeof(actual) = 'array' THEN
              IF NOT (actual @> jsonb_build_array(expected)) THEN
                rule_matches := FALSE;
              END IF;
            ELSIF actual IS DISTINCT FROM expected THEN
              rule_matches := FALSE;
            END IF;
          END IF;
          EXIT WHEN NOT rule_matches;
        END LOOP;
        IF rule_matches THEN
          selected_rule := rule;
          EXIT;
        END IF;
      END IF;
    END IF;
  END LOOP;
  IF selected_rule IS NULL THEN
    expected_effect := authority_row -> 'bundle' ->> 'default_effect';
    expected_reason_code := 'nomos.default_' || expected_effect;
    expected_matched_rule_ids := '[]'::jsonb;
    expected_obligations := '[]'::jsonb;
  ELSE
    expected_effect := selected_rule ->> 'effect';
    expected_reason_code := COALESCE(
      NULLIF(selected_rule ->> 'reason_code', ''),
      selected_rule ->> 'rule_id'
    );
    expected_matched_rule_ids := jsonb_build_array(selected_rule ->> 'rule_id');
    expected_obligations := COALESCE(selected_rule -> 'obligations', '[]'::jsonb);
  END IF;
  IF expected_effect IS NULL
     OR expected_effect NOT IN ('allow','deny','review','quarantine','redact','declassify')
     OR expected_reason_code IS NULL
     OR jsonb_typeof(expected_obligations) IS DISTINCT FROM 'array'
     OR p_command ->> 'subject' IS DISTINCT FROM authority_row ->> 'subject'
     OR p_command ->> 'effect' IS DISTINCT FROM expected_effect
     OR p_command ->> 'reason_code' IS DISTINCT FROM expected_reason_code
     OR COALESCE(p_command -> 'matched_rule_ids', '[]'::jsonb)
          IS DISTINCT FROM expected_matched_rule_ids
     OR COALESCE(p_command -> 'obligations', '[]'::jsonb)
          IS DISTINCT FROM expected_obligations THEN
    RAISE EXCEPTION 'Nomos decision is not the active database policy result'
      USING ERRCODE = '42501';
  END IF;
  expected_input_hash := 'sha256:' || encode(
    sha256(convert_to(okg.jsonb_canonical_text(jsonb_build_object(
      'subject', authority_row ->> 'subject',
      'action', p_command ->> 'action',
      'resource', COALESCE(p_command -> 'resource', '{}'::jsonb),
      'classification', p_command ->> 'classification',
      'context', COALESCE(p_command -> 'context', '{}'::jsonb)
    )), 'UTF8')), 'hex'
  );
  expected_required_obligations_digest := okg.external_action_hash(
    'okg-external-action-required-obligations-v1',
    COALESCE(p_command -> 'required_obligations', '[]'::jsonb)
  );
  expected_completed_obligations_digest := okg.external_action_hash(
    'okg-external-action-completed-obligations-v1',
    COALESCE(p_command -> 'completed_obligations', '[]'::jsonb)
  );
  expected_missing_obligations_digest := okg.external_action_hash(
    'okg-external-action-missing-obligations-v1',
    COALESCE(p_command -> 'missing_obligations', '[]'::jsonb)
  );
  IF p_command ->> 'input_hash' IS DISTINCT FROM expected_input_hash
     OR jsonb_typeof(p_command -> 'allowed') IS DISTINCT FROM 'boolean'
     OR p_command ->> 'allowed' IS DISTINCT FROM 'true'
     OR p_command ->> 'final_effect' IS DISTINCT FROM 'allow'
     OR jsonb_typeof(p_command -> 'required_obligations') IS DISTINCT FROM 'array'
     OR jsonb_typeof(p_command -> 'completed_obligations') IS DISTINCT FROM 'array'
     OR jsonb_typeof(p_command -> 'missing_obligations') IS DISTINCT FROM 'array'
     OR p_command -> 'required_obligations' IS DISTINCT FROM expected_obligations
     OR p_command -> 'completed_obligations' IS DISTINCT FROM expected_obligations
     OR p_command -> 'missing_obligations' IS DISTINCT FROM '[]'::jsonb THEN
    RAISE EXCEPTION 'Nomos strict runtime outcome is not an executable allow'
      USING ERRCODE = '42501';
  END IF;
  PERFORM okg.external_action_verify_verifier_assertion(
    p_command -> 'verifier_assertion',
    p_command ->> 'verifier_assertion_tag',
    jsonb_build_object(
      'deployment', bound_deployment,
      'operation', p_command ->> 'action',
      'nomos_decision_id', p_command ->> 'decision_id',
      'policy_fingerprint', p_command ->> 'policy_fingerprint',
      'provider_id', p_command ->> 'provider_id',
      'provider_status', p_command ->> 'provider_status',
      'provider_updated_at', to_char(
        (p_command ->> 'provider_updated_at')::timestamptz AT TIME ZONE 'UTC',
        'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
      ),
      'binding_id', p_command ->> 'binding_id',
      'binding_status', p_command ->> 'binding_status',
      'binding_updated_at', to_char(
        (p_command ->> 'binding_updated_at')::timestamptz AT TIME ZONE 'UTC',
        'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
      ),
      'nomos_input_hash', expected_input_hash,
      'allowed', true,
      'final_effect', 'allow',
      'required_obligations_digest', expected_required_obligations_digest,
      'completed_obligations_digest', expected_completed_obligations_digest,
      'missing_obligations_digest', expected_missing_obligations_digest
    )
  );
  INSERT INTO okg.nomos_decisions (
    deployment_name, decision_id, policy_bundle_id, policy_version,
    policy_fingerprint, input_hash, subject, action, resource,
    classification, context, effect, reason_code, matched_rule_ids,
    obligations, enforcement_point, target, created_at
  ) VALUES (
    bound_deployment, p_command ->> 'decision_id',
    p_command ->> 'policy_bundle_id', p_command ->> 'policy_version',
    p_command ->> 'policy_fingerprint', p_command ->> 'input_hash',
    p_command ->> 'subject', p_command ->> 'action',
    COALESCE(p_command -> 'resource', '{}'::jsonb),
    p_command ->> 'classification',
    COALESCE(p_command -> 'context', '{}'::jsonb),
    p_command ->> 'effect', p_command ->> 'reason_code',
    ARRAY(SELECT jsonb_array_elements_text(COALESCE(p_command -> 'matched_rule_ids', '[]'::jsonb))),
    COALESCE(p_command -> 'obligations', '[]'::jsonb),
    p_command ->> 'enforcement_point',
    COALESCE(p_command -> 'target', '{}'::jsonb),
    COALESCE((p_command ->> 'created_at')::timestamptz, clock_timestamp())
  ) ON CONFLICT (deployment_name, decision_id) DO NOTHING;
  GET DIAGNOSTICS inserted_count = ROW_COUNT;
  inserted := inserted_count = 1;
  SELECT * INTO decision_row
    FROM okg.nomos_decisions
   WHERE deployment_name = bound_deployment
     AND decision_id = p_command ->> 'decision_id';
  IF decision_row.decision_id IS NULL THEN
    RAISE EXCEPTION 'Nomos decision was not stored' USING ERRCODE = '40001';
  END IF;
  IF decision_row.policy_bundle_id IS DISTINCT FROM p_command ->> 'policy_bundle_id'
     OR decision_row.policy_version IS DISTINCT FROM p_command ->> 'policy_version'
     OR decision_row.policy_fingerprint IS DISTINCT FROM p_command ->> 'policy_fingerprint'
     OR decision_row.input_hash IS DISTINCT FROM p_command ->> 'input_hash'
     OR decision_row.subject IS DISTINCT FROM p_command ->> 'subject'
     OR decision_row.action IS DISTINCT FROM p_command ->> 'action'
     OR decision_row.resource IS DISTINCT FROM COALESCE(p_command -> 'resource', '{}'::jsonb)
     OR decision_row.classification IS DISTINCT FROM p_command ->> 'classification'
     OR decision_row.context IS DISTINCT FROM COALESCE(p_command -> 'context', '{}'::jsonb)
     OR decision_row.effect IS DISTINCT FROM p_command ->> 'effect'
     OR decision_row.reason_code IS DISTINCT FROM p_command ->> 'reason_code'
     OR decision_row.matched_rule_ids IS DISTINCT FROM ARRAY(
       SELECT jsonb_array_elements_text(
         COALESCE(p_command -> 'matched_rule_ids', '[]'::jsonb)
       )
     )
     OR decision_row.obligations IS DISTINCT FROM COALESCE(p_command -> 'obligations', '[]'::jsonb)
     OR decision_row.enforcement_point IS DISTINCT FROM p_command ->> 'enforcement_point'
     OR decision_row.target IS DISTINCT FROM COALESCE(p_command -> 'target', '{}'::jsonb)
     OR (p_command ->> 'created_at' IS NOT NULL
         AND decision_row.created_at IS DISTINCT FROM
           (p_command ->> 'created_at')::timestamptz) THEN
    RAISE EXCEPTION 'Nomos decision id was reused with a different request'
      USING ERRCODE = '40001';
  END IF;
  RETURN jsonb_build_object(
    'replay', NOT inserted,
    'deployment', decision_row.deployment_name,
    'decision_id', decision_row.decision_id,
    'input_hash', decision_row.input_hash,
    'subject', decision_row.subject,
    'action', decision_row.action,
    'effect', decision_row.effect,
    'reason_code', decision_row.reason_code,
    'created_at', decision_row.created_at
  );
END
$external_action_nomos_persist_decision$;

CREATE OR REPLACE FUNCTION okg.external_action_submit(p_command JSONB)
RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $external_action_submit$
DECLARE
  bound_deployment TEXT := okg.external_action_session_deployment();
  proposal_row okg.external_action_proposals%ROWTYPE;
  prior_proposal okg.external_action_proposals%ROWTYPE;
  current_lease_row okg.external_action_execution_epoch_leases%ROWTYPE;
  payload_hash TEXT;
  canonical_payload_hash TEXT;
  canonical_submission_content_hash TEXT;
  canonical_effect_instance_key TEXT;
  canonical_submission_request_hash TEXT;
  event_hash_value TEXT;
  event_projection JSONB;
  observed_at TIMESTAMPTZ := clock_timestamp();
BEGIN
  PERFORM okg.external_action_verifier_capability_status();
  PERFORM okg.external_action_require_command(
    p_command,
    ARRAY[
      'deployment','proposal_id','execution_epoch','effect_instance_key',
      'submission_content_hash','submission_request_hash',
      'submission_idempotency_key_hash','proposer_principal',
      'proposer_separation_subject','automation_id','automation_version',
      'run_id','correlation_id','action_type','target_ref','risk_tier',
      'action_schema_digest','action_contract_digest','executor_contract_digest',
      'payload_renderer_digest','governed_tool_binding_digest',
      'provider_connection_contract_digest','provider_capability_digest',
      'provider_operation_id','provider_operation_descriptor_digest',
      'original_payload_hash','requested_review_expires_at',
      'effective_review_expires_at','summary_code','evidence_digests',
      'review_state','execution_state','lifecycle_version','payload_version',
      'current_payload_hash','canonical_payload','parent_payload_hash',
      'difference_field_codes','authentication_audit_id','nomos_decision_id',
      'policy_digest','event_hash','created_at','updated_at',
      'action_contract_record'
    ],
    ARRAY[
      'deployment','proposal_id','execution_epoch','effect_instance_key',
      'submission_content_hash','submission_request_hash',
      'submission_idempotency_key_hash','proposer_principal',
      'proposer_separation_subject','automation_id','automation_version',
      'run_id','correlation_id','action_type','target_ref','risk_tier',
      'action_schema_digest','action_contract_digest','executor_contract_digest',
      'payload_renderer_digest','governed_tool_binding_digest',
      'provider_connection_contract_digest','provider_capability_digest',
      'provider_operation_id','provider_operation_descriptor_digest',
      'original_payload_hash','requested_review_expires_at',
      'effective_review_expires_at','summary_code','evidence_digests',
      'current_payload_hash','canonical_payload','authentication_audit_id',
      'nomos_decision_id','policy_digest','action_contract_record'
    ]
  );
  IF p_command ->> 'deployment' <> bound_deployment THEN
    RAISE EXCEPTION 'external action deployment is not bound to session_user'
      USING ERRCODE = '28000';
  END IF;
  PERFORM okg.external_action_nomos_authority_snapshot(
    bound_deployment, p_command ->> 'authentication_audit_id',
    p_command ->> 'nomos_decision_id', 'external_action.propose',
    p_command ->> 'submission_request_hash'
  );
  -- Every production submission retains the complete deployment-pinned
  -- contract.  Rebuild every semantic digest from that record and the
  -- canonical payload before resolving an idempotent replay.  The mapped API
  -- login is transport, never contract authority.
  IF jsonb_typeof(p_command->'action_contract_record') IS DISTINCT FROM 'object'
     OR NOT (p_command->'action_contract_record' ? 'outcome_reason_rules')
     OR jsonb_typeof(p_command->'action_contract_record'->'outcome_reason_rules') IS DISTINCT FROM 'object'
     OR p_command->'action_contract_record'->'outcome_reason_rules' = '{}'::jsonb
     OR EXISTS (
       SELECT 1
         FROM jsonb_object_keys(p_command->'action_contract_record') AS key_name
        WHERE key_name NOT IN (
          'action_type','action_schema_digest','executor_contract_digest',
          'payload_renderer_digest','governed_tool_binding_ref',
          'governed_tool_binding_digest','provider_connection_ref',
          'provider_connection_contract_digest','provider_capability_digest',
          'provider_operation_id','provider_operation_descriptor_digest',
          'provider_replay_mode','target_constraint','minimum_risk_tier',
          'review_and_execution_limits','diff_field_codes',
          'allowed_summary_codes','transition_reason_codes',
          'outcome_reason_rules'
        )
     )
     OR EXISTS (
       SELECT 1
         FROM unnest(ARRAY[
          'action_type','action_schema_digest','executor_contract_digest',
          'payload_renderer_digest','governed_tool_binding_ref',
          'governed_tool_binding_digest','provider_connection_ref',
          'provider_connection_contract_digest','provider_capability_digest',
          'provider_operation_id','provider_operation_descriptor_digest',
          'provider_replay_mode','target_constraint','minimum_risk_tier',
          'review_and_execution_limits','diff_field_codes',
          'allowed_summary_codes','transition_reason_codes',
          'outcome_reason_rules'
         ]) AS required_key
        WHERE NOT (p_command->'action_contract_record' ? required_key)
           OR p_command->'action_contract_record'->required_key = 'null'::jsonb
     ) THEN
    RAISE EXCEPTION 'action contract record is incomplete' USING ERRCODE='22023';
  END IF;
  IF p_command->>'action_type' IS DISTINCT FROM p_command->'action_contract_record'->>'action_type'
     OR p_command->>'action_schema_digest' IS DISTINCT FROM p_command->'action_contract_record'->>'action_schema_digest'
     OR p_command->>'executor_contract_digest' IS DISTINCT FROM p_command->'action_contract_record'->>'executor_contract_digest'
     OR p_command->>'payload_renderer_digest' IS DISTINCT FROM p_command->'action_contract_record'->>'payload_renderer_digest'
     OR p_command->>'governed_tool_binding_digest' IS DISTINCT FROM p_command->'action_contract_record'->>'governed_tool_binding_digest'
     OR p_command->>'provider_connection_contract_digest' IS DISTINCT FROM p_command->'action_contract_record'->>'provider_connection_contract_digest'
     OR p_command->>'provider_capability_digest' IS DISTINCT FROM p_command->'action_contract_record'->>'provider_capability_digest'
     OR p_command->>'provider_operation_id' IS DISTINCT FROM p_command->'action_contract_record'->>'provider_operation_id'
     OR p_command->>'provider_operation_descriptor_digest' IS DISTINCT FROM p_command->'action_contract_record'->>'provider_operation_descriptor_digest'
     OR (CASE p_command->>'risk_tier'
          WHEN 'low' THEN 0 WHEN 'moderate' THEN 1 WHEN 'high' THEN 2 WHEN 'critical' THEN 3 ELSE -1
        END) < (CASE p_command->'action_contract_record'->>'minimum_risk_tier'
          WHEN 'low' THEN 0 WHEN 'moderate' THEN 1 WHEN 'high' THEN 2 WHEN 'critical' THEN 3 ELSE 99
        END) THEN
    RAISE EXCEPTION 'submission fields do not match the retained action contract' USING ERRCODE='22023';
  END IF;
  BEGIN
    IF p_command->>'action_contract_digest' IS DISTINCT FROM okg.external_action_hash(
      'okg-external-action-contract-v1', p_command->'action_contract_record') THEN
      RAISE EXCEPTION 'action contract digest is not canonical' USING ERRCODE='22023';
    END IF;
    canonical_payload_hash:=okg.external_action_hash(
      'okg-external-action-payload-v1',
      convert_from(okg.external_action_json_bytes(p_command->'canonical_payload'),'UTF8')::jsonb);
    IF p_command->>'original_payload_hash' IS DISTINCT FROM canonical_payload_hash
       OR p_command->>'current_payload_hash' IS DISTINCT FROM canonical_payload_hash THEN
      RAISE EXCEPTION 'submission payload hash is not canonical' USING ERRCODE='22023';
    END IF;
    canonical_submission_content_hash:=okg.external_action_hash(
      'okg-external-action-submission-content-v1',jsonb_build_object(
        'automation_id',p_command->>'automation_id','automation_version',p_command->>'automation_version',
        'run_id',p_command->>'run_id','correlation_id',p_command->>'correlation_id',
        'action_type',p_command->>'action_type','target_ref',p_command->>'target_ref',
        'risk_tier',p_command->>'risk_tier','action_schema_digest',p_command->>'action_schema_digest',
        'action_contract_digest',p_command->>'action_contract_digest',
        'original_payload_hash',canonical_payload_hash,
        'requested_review_expires_at',to_char((p_command->>'requested_review_expires_at')::timestamptz AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
        'summary_code',p_command->>'summary_code','evidence_digests',p_command->'evidence_digests'));
    canonical_effect_instance_key:=okg.external_action_hash(
      'okg-external-action-effect-instance-v1',jsonb_build_object(
        'deployment',bound_deployment,
        'proposer_separation_subject',p_command->>'proposer_separation_subject',
        'submission_idempotency_key_hash',p_command->>'submission_idempotency_key_hash'));
    canonical_submission_request_hash:=okg.external_action_hash(
      'okg-external-action-proposal-request-v1',jsonb_build_object(
        'deployment',bound_deployment,'execution_epoch',(p_command->>'execution_epoch')::bigint,
        'effect_instance_key',canonical_effect_instance_key,
        'submission_content_hash',canonical_submission_content_hash,
        'proposer_separation_subject',p_command->>'proposer_separation_subject',
        'automation_id',p_command->>'automation_id','automation_version',p_command->>'automation_version',
        'run_id',p_command->>'run_id','correlation_id',p_command->>'correlation_id',
        'action_type',p_command->>'action_type','target_ref',p_command->>'target_ref',
        'risk_tier',p_command->>'risk_tier','action_schema_digest',p_command->>'action_schema_digest',
        'action_contract_digest',p_command->>'action_contract_digest',
        'executor_contract_digest',p_command->>'executor_contract_digest',
        'payload_renderer_digest',p_command->>'payload_renderer_digest',
        'governed_tool_binding_digest',p_command->>'governed_tool_binding_digest',
        'provider_connection_contract_digest',p_command->>'provider_connection_contract_digest',
        'provider_capability_digest',p_command->>'provider_capability_digest',
        'provider_operation_id',p_command->>'provider_operation_id',
        'provider_operation_descriptor_digest',p_command->>'provider_operation_descriptor_digest',
        'original_payload_hash',canonical_payload_hash,
        'requested_review_expires_at',to_char((p_command->>'requested_review_expires_at')::timestamptz AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
        'summary_code',p_command->>'summary_code','evidence_digests',p_command->'evidence_digests'));
    IF p_command->>'effect_instance_key' IS DISTINCT FROM canonical_effect_instance_key
       OR p_command->>'submission_content_hash' IS DISTINCT FROM canonical_submission_content_hash
       OR p_command->>'submission_request_hash' IS DISTINCT FROM canonical_submission_request_hash THEN
      RAISE EXCEPTION 'submission semantic hash is not canonical' USING ERRCODE='22023';
    END IF;
  END;
  PERFORM pg_advisory_xact_lock(
    hashtextextended('okg.external_action.deployment:' || bound_deployment, 0)
  );
  SELECT * INTO prior_proposal
    FROM okg.external_action_proposals
   WHERE deployment_name = bound_deployment
     AND proposer_separation_subject = p_command ->> 'proposer_separation_subject'
     AND submission_idempotency_key_hash = p_command ->> 'submission_idempotency_key_hash'
   FOR UPDATE;
  IF prior_proposal.proposal_id IS NOT NULL THEN
    IF prior_proposal.submission_request_hash <> p_command ->> 'submission_request_hash' THEN
      RAISE EXCEPTION 'submission idempotency key was reused with a different request'
        USING ERRCODE = '40001';
    END IF;
    RETURN jsonb_build_object(
      'replay', TRUE, 'proposal_id', prior_proposal.proposal_id,
      'deployment', prior_proposal.deployment_name,
      'effect_instance_key', prior_proposal.effect_instance_key,
      'review_state', prior_proposal.review_state,
      'execution_state', prior_proposal.execution_state,
      'lifecycle_version', prior_proposal.lifecycle_version,
      'payload_version', prior_proposal.payload_version,
      'current_payload_hash', prior_proposal.current_payload_hash
    );
  END IF;
  -- A new submission is checked against one database-clock observation and
  -- the currently accepted, unexpired execution lease.  Keep this after the
  -- exact idempotent replay above: retries must resolve even after a deadline
  -- or lease boundary has passed.
  IF (p_command ->> 'requested_review_expires_at')::timestamptz <= observed_at
     OR (p_command ->> 'effective_review_expires_at')::timestamptz <= observed_at
     OR (p_command ->> 'effective_review_expires_at')::timestamptz
          > (p_command ->> 'requested_review_expires_at')::timestamptz THEN
    RAISE EXCEPTION 'external action review deadline interval is invalid'
      USING ERRCODE = '22023';
  END IF;
  SELECT * INTO current_lease_row
    FROM okg.external_action_execution_epoch_leases
   WHERE deployment_name = bound_deployment
   ORDER BY epoch DESC, lease_sequence DESC
   LIMIT 1;
  IF current_lease_row.lease_row_id IS NULL
     OR current_lease_row.epoch <> (p_command ->> 'execution_epoch')::bigint
     OR current_lease_row.issued_at > observed_at
     OR current_lease_row.expires_at <= observed_at THEN
    RAISE EXCEPTION 'external action execution epoch lease is not current'
      USING ERRCODE = '40001';
  END IF;
  payload_hash := p_command ->> 'current_payload_hash';
  INSERT INTO okg.external_action_proposals (
    proposal_id,deployment_name,execution_epoch,effect_instance_key,
    submission_content_hash,submission_request_hash,
    submission_idempotency_key_hash,proposer_separation_subject,
    automation_id,automation_version,run_id,correlation_id,action_type,
    target_ref,risk_tier,action_schema_digest,action_contract_digest,
    executor_contract_digest,payload_renderer_digest,
    governed_tool_binding_digest,provider_connection_contract_digest,
    provider_capability_digest,provider_operation_id,
    provider_operation_descriptor_digest,original_payload_hash,
    requested_review_expires_at,effective_review_expires_at,summary_code,
    evidence_digests,review_state,execution_state,lifecycle_version,
    payload_version,current_payload_hash,created_at,updated_at,
    store_record,action_contract_record,submission_idempotency_record
  ) VALUES (
    p_command ->> 'proposal_id',bound_deployment,(p_command ->> 'execution_epoch')::bigint,
    p_command ->> 'effect_instance_key',p_command ->> 'submission_content_hash',
    p_command ->> 'submission_request_hash',p_command ->> 'submission_idempotency_key_hash',
    p_command ->> 'proposer_separation_subject',p_command ->> 'automation_id',
    p_command ->> 'automation_version',p_command ->> 'run_id',p_command ->> 'correlation_id',
    p_command ->> 'action_type',p_command ->> 'target_ref',p_command ->> 'risk_tier',
    p_command ->> 'action_schema_digest',p_command ->> 'action_contract_digest',
    p_command ->> 'executor_contract_digest',p_command ->> 'payload_renderer_digest',
    p_command ->> 'governed_tool_binding_digest',p_command ->> 'provider_connection_contract_digest',
    p_command ->> 'provider_capability_digest',p_command ->> 'provider_operation_id',
    p_command ->> 'provider_operation_descriptor_digest',p_command ->> 'original_payload_hash',
    (p_command ->> 'requested_review_expires_at')::timestamptz,
    (p_command ->> 'effective_review_expires_at')::timestamptz,
    p_command ->> 'summary_code',p_command -> 'evidence_digests',
    COALESCE(p_command ->> 'review_state','pending'),
    COALESCE(p_command ->> 'execution_state','unavailable'),
    COALESCE((p_command ->> 'lifecycle_version')::bigint,1),
    COALESCE((p_command ->> 'payload_version')::bigint,1),
    payload_hash,observed_at,observed_at,p_command,p_command -> 'action_contract_record',
    jsonb_build_object('key', p_command ->> 'submission_idempotency_key_hash',
                       'value', p_command ->> 'submission_request_hash')
  ) RETURNING * INTO proposal_row;
  INSERT INTO okg.external_action_payload_versions (
    deployment_name,proposal_id,payload_version,payload_hash,parent_payload_hash,
    canonical_payload,difference_field_codes,created_at,store_record
  ) VALUES (
    bound_deployment,proposal_row.proposal_id,proposal_row.payload_version,
    payload_hash,p_command ->> 'parent_payload_hash',
    okg.external_action_json_bytes(p_command -> 'canonical_payload'),
    COALESCE(p_command -> 'difference_field_codes','[]'::jsonb),
    observed_at,p_command
  );
  event_projection := jsonb_build_object(
    'proposal_id', proposal_row.proposal_id,
    'execution_epoch', proposal_row.execution_epoch,
    'effect_instance_key', proposal_row.effect_instance_key,
    'sequence', 1,
    'lifecycle_version', proposal_row.lifecycle_version,
    'event_type', 'submitted',
    'actor_kind', 'authenticated_principal',
    'actor_principal', p_command ->> 'proposer_principal',
    'actor_separation_subject', p_command ->> 'proposer_separation_subject',
    'authentication_audit_ref', p_command ->> 'authentication_audit_id',
    'nomos_decision_ref', p_command ->> 'nomos_decision_id',
    'deadline_kind', NULL::TEXT,
    'policy_digest', p_command ->> 'policy_digest',
    'before_review_state', NULL::TEXT,
    'after_review_state', proposal_row.review_state,
    'before_execution_state', NULL::TEXT,
    'after_execution_state', proposal_row.execution_state,
    'payload_version', proposal_row.payload_version,
    'payload_hash', payload_hash,
    'decision_id', NULL::TEXT,
    'decision_binding_hash', NULL::TEXT,
    'execution_id', NULL::TEXT,
    'execution_binding_hash', NULL::TEXT,
    'transition_reason_code', NULL::TEXT,
    'outcome', NULL::TEXT,
    'outcome_reason_code', NULL::TEXT,
    'outcome_request_hash', NULL::TEXT,
    'restore_recovery_digest', NULL::TEXT,
    'effective_at', NULL::TEXT,
    'recorded_at', to_char(observed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
    'previous_event_hash', NULL::TEXT
  );
  event_hash_value := okg.external_action_hash('okg-external-action-event-v1', event_projection);
  IF p_command ->> 'event_hash' IS NOT NULL
     AND p_command ->> 'event_hash' IS DISTINCT FROM event_hash_value THEN
    RAISE EXCEPTION 'caller-supplied event hash disagrees with the closed event projection'
      USING ERRCODE = '22023';
  END IF;
  INSERT INTO okg.external_action_events (
    deployment_name,proposal_id,execution_epoch,effect_instance_key,sequence,
    lifecycle_version,event_type,actor_kind,actor_principal,
    actor_separation_subject,authentication_audit_ref,nomos_decision_ref,
    policy_digest,before_review_state,after_review_state,before_execution_state,
    after_execution_state,payload_version,payload_hash,effective_at,recorded_at,
    previous_event_hash,event_hash,store_record
  ) VALUES (
    bound_deployment,proposal_row.proposal_id,proposal_row.execution_epoch,
    proposal_row.effect_instance_key,1,proposal_row.lifecycle_version,
    'submitted','authenticated_principal',p_command ->> 'proposer_principal',
    p_command ->> 'proposer_separation_subject',
    p_command ->> 'authentication_audit_id',p_command ->> 'nomos_decision_id',
    p_command ->> 'policy_digest',NULL,proposal_row.review_state,NULL,
    proposal_row.execution_state,proposal_row.payload_version,payload_hash,
    NULL,observed_at,NULL,event_hash_value,
    jsonb_build_object('event_projection', event_projection)
  );
  RETURN jsonb_build_object(
    'replay',FALSE,'proposal_id',proposal_row.proposal_id,
    'deployment',proposal_row.deployment_name,'effect_instance_key',proposal_row.effect_instance_key,
    'review_state',proposal_row.review_state,'execution_state',proposal_row.execution_state,
    'lifecycle_version',proposal_row.lifecycle_version,'payload_version',proposal_row.payload_version,
    'current_payload_hash',proposal_row.current_payload_hash,'event_hash',event_hash_value
  );
END
$external_action_submit$;
CREATE OR REPLACE FUNCTION okg.external_action_logical_json_record(p_record JSONB)
RETURNS JSONB
LANGUAGE sql IMMUTABLE
SET search_path = pg_catalog, okg
AS $external_action_logical_json_record$
  SELECT CASE
    WHEN p_record ? 'deployment_name' THEN
      (p_record - 'deployment_name')
      || jsonb_build_object('deployment', p_record -> 'deployment_name')
    ELSE p_record
  END
$external_action_logical_json_record$;
REVOKE ALL ON FUNCTION okg.external_action_logical_json_record(JSONB) FROM PUBLIC;

CREATE OR REPLACE FUNCTION okg.external_action_store_snapshot()
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, okg AS $$
DECLARE bound_deployment TEXT := okg.external_action_session_deployment();
BEGIN
  RETURN jsonb_build_object(
    'execution_epoch', COALESCE((SELECT max(epoch) FROM okg.external_action_execution_epoch_leases WHERE deployment_name = bound_deployment), 0),
    'execution_epoch_lease_digest', (SELECT envelope_digest FROM okg.external_action_execution_epoch_leases WHERE deployment_name = bound_deployment ORDER BY epoch DESC, lease_sequence DESC LIMIT 1),
    'proposals', COALESCE((SELECT jsonb_agg(jsonb_build_object('key', p.proposal_id, 'value', okg.external_action_logical_json_record(to_jsonb(p)) || jsonb_build_object('proposer_principal', p.proposer_separation_subject)) ORDER BY p.proposal_id) FROM okg.external_action_proposals p WHERE p.deployment_name = bound_deployment), '[]'::jsonb),
    'payload_versions', COALESCE((SELECT jsonb_agg(jsonb_build_object('bucket', v.proposal_id, 'value', okg.external_action_logical_json_record(to_jsonb(v)) || jsonb_build_object('version', v.payload_version, 'difference', v.difference_field_codes, 'canonical_payload', jsonb_build_object('__okg_bytes_b64', encode(v.canonical_payload, 'base64'))) ) ORDER BY v.proposal_id, v.payload_version) FROM okg.external_action_payload_versions v WHERE v.deployment_name = bound_deployment), '[]'::jsonb),
    'decisions', COALESCE((SELECT jsonb_agg(jsonb_build_object('bucket', d.proposal_id, 'value', jsonb_build_object('decision_id', d.decision_id, 'proposal_id', d.proposal_id, 'verdict', CASE WHEN d.verdict = 'approved' THEN 'approve' ELSE 'reject' END, 'lifecycle_version', d.approved_lifecycle_version, 'payload_version', d.approved_payload_version, 'payload_hash', d.approved_payload_hash, 'approver_principal', d.approver_principal, 'approver_separation_subject', d.approver_separation_subject, 'policy_digest', d.policy_digest, 'execute_before', d.execute_before, 'reason_code', d.reason_code, 'decision_binding_hash', d.decision_binding_hash, 'created_at', d.created_at)) ORDER BY d.proposal_id, d.decision_row_id) FROM okg.external_action_decisions d WHERE d.deployment_name = bound_deployment), '[]'::jsonb),
    'events', COALESCE((SELECT jsonb_agg(jsonb_build_object('bucket', e.effect_instance_key, 'value', okg.external_action_logical_json_record(to_jsonb(e))) ORDER BY e.effect_instance_key, e.sequence) FROM okg.external_action_events e WHERE e.deployment_name = bound_deployment), '[]'::jsonb),
    'restore_fences', COALESCE((SELECT jsonb_agg(jsonb_build_object('key', f.deployment_name, 'value', okg.external_action_logical_json_record(to_jsonb(f))) ORDER BY f.deployment_name) FROM okg.external_action_restore_fences f WHERE f.deployment_name = bound_deployment), '[]'::jsonb)
  );
END
$$;

-- The old generic mirror writer is not a durability boundary.  Typed RPCs
-- above are the only accepted production write surface; remove the legacy
-- entry point when upgrading a database that still has it.
DROP FUNCTION IF EXISTS okg.external_action_store_append(TEXT, JSONB);

CREATE OR REPLACE FUNCTION okg.external_action_store_resolve_principal(p_issuer TEXT, p_subject TEXT)
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, okg AS $$
DECLARE bound_deployment TEXT := okg.external_action_session_deployment(); result JSONB;
BEGIN
  PERFORM okg.external_action_verifier_capability_status();
  SELECT jsonb_build_object('principal', b.subject, 'separation_subject', b.subject, 'provider_id', p.provider_id, 'subject_binding_id', b.binding_id, 'issuer', p.issuer)
    INTO result
    FROM okg.nomos_identity_providers p
    JOIN okg.nomos_subject_bindings b ON b.deployment_name = p.deployment_name AND b.provider_id = p.provider_id AND b.issuer = p.issuer
   WHERE p.deployment_name = bound_deployment AND p.issuer = p_issuer AND p.status = 'active' AND b.status = 'active' AND b.claim_path = p.subject_claim AND b.claim_value = p_subject AND (b.effective_from IS NULL OR b.effective_from <= clock_timestamp()) AND (b.effective_until IS NULL OR clock_timestamp() < b.effective_until)
   ORDER BY b.priority DESC, b.binding_id
   LIMIT 1;
  IF result IS NULL THEN
    RAISE EXCEPTION 'deployment-pinned principal mapping is unavailable' USING ERRCODE = '28000';
  END IF;
  RETURN result;
END
$$;

CREATE OR REPLACE FUNCTION okg.external_action_insert_dpop_replay_nonce(p_deployment TEXT, p_issuer_digest TEXT, p_sender_key_thumbprint TEXT, p_jti_digest TEXT, p_request_method TEXT, p_request_uri_digest TEXT, p_access_token_hash TEXT, p_observed_at TIMESTAMPTZ, p_expires_at TIMESTAMPTZ)
RETURNS BOOLEAN LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, okg AS $$
DECLARE bound_deployment TEXT := okg.external_action_session_deployment(); inserted_count INTEGER;
BEGIN
  PERFORM okg.external_action_verifier_capability_status();
  IF p_deployment IS NULL OR p_deployment <> bound_deployment THEN
    RAISE EXCEPTION 'external action deployment is not bound to session_user' USING ERRCODE = '28000';
  END IF;
  INSERT INTO okg.external_action_dpop_replay_nonces (deployment_name, issuer_digest, sender_key_thumbprint, jti_digest, request_method, normalized_request_uri_digest, access_token_hash, observed_at, expires_at)
  VALUES (bound_deployment, p_issuer_digest, p_sender_key_thumbprint, p_jti_digest, p_request_method, p_request_uri_digest, p_access_token_hash, p_observed_at, p_expires_at)
  ON CONFLICT (deployment_name, issuer_digest, sender_key_thumbprint, jti_digest) DO NOTHING;
  GET DIAGNOSTICS inserted_count = ROW_COUNT;
  RETURN inserted_count = 1;
END
$$;

CREATE OR REPLACE FUNCTION okg.external_action_store_startup(p_state JSONB)
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, okg AS $$
DECLARE bound_deployment TEXT := okg.external_action_session_deployment(); requested TEXT;
BEGIN
  requested := p_state->>'deployment';
  IF requested IS NULL OR requested <> bound_deployment THEN
    RAISE EXCEPTION 'external action deployment is not bound to session_user' USING ERRCODE = '28000';
  END IF;
  RETURN jsonb_build_object('accepted', true, 'deployment', bound_deployment);
END
$$;

DO $external_action_runtime_function_grants$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'okg_external_action_api') THEN
    GRANT EXECUTE ON FUNCTION okg.external_action_store_snapshot() TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_nomos_policy_bundle(TEXT,TEXT,TEXT) TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_store_resolve_principal(TEXT, TEXT) TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_claim_owner_unavailability(TEXT) TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_insert_dpop_replay_nonce(TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TIMESTAMPTZ, TIMESTAMPTZ) TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_startup_login_binding() TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_store_startup(JSONB) TO okg_external_action_api;
  END IF;
  IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'okg_external_action_function_owner') THEN
    GRANT SELECT, INSERT, UPDATE ON
      okg.external_action_proposals,
      okg.external_action_payload_versions,
      okg.external_action_auth_audits,
      okg.external_action_dpop_replay_nonces,
      okg.external_action_decisions,
      okg.external_action_transition_idempotency,
      okg.external_action_executions,
      okg.external_action_outcomes,
      okg.external_action_events,
      okg.external_action_owner_unavailability_attestations,
      okg.external_action_execution_epoch_leases,
      okg.external_action_restore_attestations,
      okg.external_action_restore_fences,
      okg.external_action_restore_sessions,
      okg.external_action_restore_checkpoint_staging,
      okg.external_action_restore_completion_receipts,
      okg.external_action_restore_finalize_idempotency,
      okg.external_action_restore_checkpoint_entries,
      okg.external_action_restore_recoveries
    TO okg_external_action_function_owner;
    GRANT SELECT, UPDATE ON okg.external_action_api_role_bindings TO okg_external_action_function_owner;
    GRANT SELECT ON okg.nomos_policy_bundles, okg.nomos_policy_activations, okg.nomos_identity_providers, okg.nomos_subject_bindings, okg.nomos_decisions TO okg_external_action_function_owner;
    GRANT USAGE, SELECT ON
      okg.external_action_decisions_decision_row_id_seq,
      okg.external_action_dpop_replay_nonces_dpop_replay_nonce_id_seq,
      okg.external_action_events_event_id_seq,
      okg.external_action_execution_epoch_leases_lease_row_id_seq,
      okg.external_action_outcomes_outcome_id_seq,
      okg.external_action_owner_unavailability_attesta_attestation_id_seq,
      okg.external_action_payload_versions_payload_version_id_seq,
      okg.external_action_restore_attestations_attestation_row_id_seq,
      okg.external_action_restore_checkpoint__checkpoint_entry_row_id_seq,
      okg.external_action_restore_checkpoin_checkpoint_staging_row_id_seq,
      okg.external_action_restore_recoveries_recovery_row_id_seq,
      okg.external_action_restore_sessions_restore_session_row_id_seq,
      okg.external_action_restore_completio_completion_receipt_row_id_seq,
      okg.external_action_restore_finaliz_finalize_idempotency_row_id_seq,
      okg.external_action_transition_idempo_transition_idempotency_id_seq
    TO okg_external_action_function_owner;
    REVOKE ALL ON FUNCTION okg.external_action_session_deployment() FROM PUBLIC;
    REVOKE ALL ON FUNCTION okg.external_action_assert_deployment(TEXT) FROM PUBLIC;
    REVOKE ALL ON FUNCTION okg.external_action_store_snapshot() FROM PUBLIC;
    REVOKE ALL ON FUNCTION okg.external_action_nomos_policy_bundle(TEXT,TEXT,TEXT) FROM PUBLIC;
    REVOKE ALL ON FUNCTION okg.external_action_store_resolve_principal(TEXT, TEXT) FROM PUBLIC;
    REVOKE ALL ON FUNCTION okg.external_action_insert_dpop_replay_nonce(TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TIMESTAMPTZ, TIMESTAMPTZ) FROM PUBLIC;
    REVOKE ALL ON FUNCTION okg.external_action_startup_login_binding() FROM PUBLIC;
    REVOKE ALL ON FUNCTION okg.external_action_claim_owner_unavailability(TEXT) FROM PUBLIC;
    REVOKE ALL ON FUNCTION okg.external_action_store_startup(JSONB) FROM PUBLIC;
    ALTER FUNCTION okg.external_action_session_deployment() OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_assert_deployment(TEXT) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_store_snapshot() OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_nomos_policy_bundle(TEXT,TEXT,TEXT) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_nomos_authority_lock() OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_store_resolve_principal(TEXT, TEXT) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_insert_dpop_replay_nonce(TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TIMESTAMPTZ, TIMESTAMPTZ) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_startup_login_binding() OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_store_startup(JSONB) OWNER TO okg_external_action_function_owner;
  END IF;
END
$external_action_runtime_function_grants$;
CREATE OR REPLACE FUNCTION okg.external_action_review_transition(
  p_command JSONB
) RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $external_action_review_transition$
DECLARE
  bound_deployment TEXT := okg.external_action_session_deployment();
  proposal_row okg.external_action_proposals%ROWTYPE;
  previous_proposal okg.external_action_proposals%ROWTYPE;
  fence_row okg.external_action_restore_fences%ROWTYPE;
  replay_row okg.external_action_transition_idempotency%ROWTYPE;
  decision_row okg.external_action_decisions%ROWTYPE;
  operation_value TEXT;
  next_review_state TEXT;
  next_execution_state TEXT;
  next_lifecycle BIGINT;
  next_payload_version BIGINT;
  next_payload_hash TEXT;
  decision_id_value TEXT;
  decision_binding_value TEXT;
  deadline_value TIMESTAMPTZ;
  observed_at TIMESTAMPTZ := clock_timestamp();
  sequence_value BIGINT;
  previous_event_hash_value TEXT;
  event_hash_value TEXT;
  event_projection JSONB;
  response_body_value BYTEA;
  canonical_transition_request_hash TEXT;
  canonical_replacement_payload_hash TEXT;
BEGIN
  PERFORM okg.external_action_verifier_capability_status();
  PERFORM okg.external_action_require_command(
    p_command,
    ARRAY[
      'deployment','proposal_id','operation','actor_principal',
      'actor_separation_subject','idempotency_key_hash',
      'transition_request_hash','authentication_audit_id','nomos_decision_id',
      'policy_digest','expected_lifecycle_version','expected_payload_version',
      'expected_payload_hash','expected_decision_id',
      'expected_decision_binding_hash','replacement_payload',
      'canonical_payload','replacement_payload_hash','payload_version',
      'reason_code','execute_before','decision_id','decision_binding_hash',
      'event_hash','previous_event_hash','response_status',
      'response_media_type','response_body_jcs','result_event_ids',
      'result_payload_version','result_decision_id','result_lifecycle_version',
      'result_etag','action_contract_digest','action_contract_record'
    ],
    ARRAY[
      'deployment','proposal_id','operation','actor_principal',
      'actor_separation_subject','idempotency_key_hash',
      'transition_request_hash','authentication_audit_id','nomos_decision_id',
      'policy_digest'
    ]
  );
  IF p_command ->> 'deployment' <> bound_deployment THEN
    RAISE EXCEPTION 'external action deployment is not bound to session_user'
      USING ERRCODE = '28000';
  END IF;
  operation_value := p_command ->> 'operation';
  IF operation_value NOT IN ('edit','defer','approve','approve_with_edit','reject','cancel') THEN
    RAISE EXCEPTION 'external action transition operation is not allowed'
      USING ERRCODE = '22023';
  END IF;
  PERFORM pg_advisory_xact_lock(
    hashtextextended('okg.external_action.deployment:' || bound_deployment, 0)
  );
  SELECT * INTO proposal_row
    FROM okg.external_action_proposals
   WHERE deployment_name = bound_deployment
     AND proposal_id = p_command ->> 'proposal_id'
   FOR UPDATE;
  IF proposal_row.proposal_id IS NULL THEN
    RAISE EXCEPTION 'external action proposal was not found'
      USING ERRCODE = 'P0002';
  END IF;
  previous_proposal := proposal_row;
  -- Current Nomos authority is checked before resolving a transition retry;
  -- a stale exact request must not become an authorization oracle.
  PERFORM okg.external_action_nomos_authority_snapshot(
    bound_deployment,
    p_command ->> 'authentication_audit_id',
    p_command ->> 'nomos_decision_id',
    CASE WHEN operation_value = 'cancel'
         THEN 'external_action.cancel' ELSE 'external_action.decide' END,
    p_command ->> 'transition_request_hash'
  );
  IF proposal_row.action_contract_record ? 'outcome_reason_rules' THEN
    canonical_replacement_payload_hash:=NULL;
    IF p_command->>'replacement_payload_hash' IS NOT NULL THEN
      IF p_command->>'canonical_payload' IS NULL THEN
        RAISE EXCEPTION 'replacement payload bytes are required' USING ERRCODE='22023';
      END IF;
      canonical_replacement_payload_hash:=okg.external_action_hash(
        'okg-external-action-payload-v1',
        convert_from(okg.external_action_json_bytes(p_command->'canonical_payload'),'UTF8')::jsonb);
      IF p_command->>'replacement_payload_hash' IS DISTINCT FROM canonical_replacement_payload_hash THEN
        RAISE EXCEPTION 'replacement payload hash is not canonical' USING ERRCODE='22023';
      END IF;
    END IF;
  END IF;
  SELECT * INTO replay_row
    FROM okg.external_action_transition_idempotency
   WHERE deployment_name = bound_deployment
     AND proposal_id = proposal_row.proposal_id
     AND actor_separation_subject = p_command ->> 'actor_separation_subject'
     AND idempotency_key_hash = p_command ->> 'idempotency_key_hash'
   FOR UPDATE;
  IF replay_row.transition_idempotency_id IS NOT NULL THEN
    IF replay_row.transition_request_hash <> p_command ->> 'transition_request_hash' THEN
      RAISE EXCEPTION 'transition idempotency key was reused with a different request'
        USING ERRCODE = '40001';
    END IF;
    RETURN COALESCE(
      replay_row.store_record -> 'response_projection',
      jsonb_build_object(
        'replay', TRUE, 'proposal_id', proposal_row.proposal_id,
        'deployment', bound_deployment,
        'response_status', replay_row.response_status
      )
    ) || jsonb_build_object('replay', TRUE);
  END IF;
  -- A transition is a lifecycle mutation. Check the persisted startup fence
  -- after the authority/replay checks, while retaining the deployment
  -- advisory lock, so restore quarantine cannot be bypassed by updating an
  -- existing proposal instead of inserting a new row.
  SELECT * INTO fence_row
    FROM okg.external_action_restore_fences
   WHERE deployment_name = bound_deployment
   FOR SHARE;
  IF fence_row.deployment_name IS NOT NULL
     AND (fence_row.service_mode <> 'normal'
          OR fence_row.quarantine_state <> 'clear') THEN
    RAISE EXCEPTION 'external action lifecycle is quarantined for restore'
      USING ERRCODE = '40001';
  END IF;
  IF fence_row.deployment_name IS NOT NULL
     AND proposal_row.execution_epoch < fence_row.highest_accepted_epoch THEN
    RAISE EXCEPTION 'external action lifecycle epoch is stale'
      USING ERRCODE = '40001';
  END IF;
  IF operation_value <> 'cancel'
     AND p_command ->> 'actor_separation_subject'
         = proposal_row.proposer_separation_subject THEN
    RAISE EXCEPTION 'proposer and approver separation is required'
      USING ERRCODE = '42501';
  END IF;
  IF operation_value = 'cancel'
     AND p_command ->> 'actor_separation_subject'
         <> proposal_row.proposer_separation_subject THEN
    RAISE EXCEPTION 'only the proposer may cancel an external action'
      USING ERRCODE = '42501';
  END IF;
  IF p_command ? 'expected_lifecycle_version'
     AND (p_command ->> 'expected_lifecycle_version')::bigint
         <> proposal_row.lifecycle_version THEN
    RAISE EXCEPTION 'external action lifecycle precondition is stale'
      USING ERRCODE = '40001';
  END IF;
  IF p_command ? 'expected_payload_version'
     AND (p_command ->> 'expected_payload_version')::bigint
         <> proposal_row.payload_version THEN
    RAISE EXCEPTION 'external action payload precondition is stale'
      USING ERRCODE = '40001';
  END IF;
  IF p_command ? 'expected_payload_hash'
     AND p_command ->> 'expected_payload_hash'
         <> proposal_row.current_payload_hash THEN
    RAISE EXCEPTION 'external action payload binding is stale'
      USING ERRCODE = '40001';
  END IF;
  IF p_command ? 'expected_decision_id'
     AND p_command ->> 'expected_decision_id'
         IS DISTINCT FROM proposal_row.current_decision_id THEN
    RAISE EXCEPTION 'external action decision binding is stale'
      USING ERRCODE = '40001';
  END IF;
  IF p_command ? 'expected_decision_binding_hash'
     AND p_command ->> 'expected_decision_binding_hash'
         IS DISTINCT FROM proposal_row.current_decision_binding_hash THEN
    RAISE EXCEPTION 'external action decision binding is stale'
      USING ERRCODE = '40001';
  END IF;
  IF operation_value IN ('edit','defer','approve','approve_with_edit','reject')
     AND proposal_row.review_state NOT IN ('pending','edited') THEN
    RAISE EXCEPTION 'external action proposal is not reviewable'
      USING ERRCODE = '40001';
  END IF;
  IF operation_value = 'cancel'
     AND (proposal_row.review_state NOT IN ('pending','edited','approved')
          OR EXISTS (
            SELECT 1 FROM okg.external_action_executions e
             WHERE e.deployment_name = bound_deployment
               AND e.proposal_id = proposal_row.proposal_id
          )) THEN
    RAISE EXCEPTION 'external action proposal cannot be cancelled'
      USING ERRCODE = '40001';
  END IF;
  IF operation_value IN ('edit','defer','approve','approve_with_edit','reject')
     AND observed_at >= proposal_row.effective_review_expires_at THEN
    RAISE EXCEPTION 'external action review deadline has passed'
      USING ERRCODE = '22008';
  END IF;
  next_review_state := proposal_row.review_state;
  next_execution_state := proposal_row.execution_state;
  next_lifecycle := proposal_row.lifecycle_version + 1;
  next_payload_version := proposal_row.payload_version;
  next_payload_hash := proposal_row.current_payload_hash;
  IF operation_value IN ('edit','approve_with_edit') THEN
    IF NOT (p_command ? 'canonical_payload')
       OR NOT (p_command ? 'replacement_payload_hash') THEN
      RAISE EXCEPTION 'replacement payload and hash are required'
        USING ERRCODE = '22023';
    END IF;
    next_payload_version := proposal_row.payload_version + 1;
    next_payload_hash := p_command ->> 'replacement_payload_hash';
    INSERT INTO okg.external_action_payload_versions (
      deployment_name, proposal_id, payload_version, payload_hash,
      parent_payload_hash, canonical_payload, difference_field_codes,
      created_at, store_record
    ) VALUES (
      bound_deployment, proposal_row.proposal_id, next_payload_version,
      next_payload_hash, proposal_row.current_payload_hash,
      okg.external_action_json_bytes(p_command -> 'canonical_payload'),
      COALESCE(p_command -> 'result_event_ids', '[]'::jsonb),
      observed_at, p_command
    );
    next_review_state := 'edited';
  END IF;
  IF operation_value IN ('approve','approve_with_edit') THEN
    IF NOT (p_command ? 'decision_id')
       OR NOT (p_command ? 'execute_before') THEN
      RAISE EXCEPTION 'approval decision and deadline are required'
        USING ERRCODE = '22023';
    END IF;
    decision_id_value := p_command ->> 'decision_id';
    deadline_value := (p_command ->> 'execute_before')::timestamptz;
    IF deadline_value <= observed_at THEN
      RAISE EXCEPTION 'execution deadline has passed'
        USING ERRCODE = '22008';
    END IF;
    decision_binding_value := okg.external_action_hash(
      'okg-external-action-decision-binding-v1',
      jsonb_build_object(
        'deployment', bound_deployment,
        'execution_epoch', proposal_row.execution_epoch,
        'effect_instance_key', proposal_row.effect_instance_key,
        'proposal_id', proposal_row.proposal_id,
        'decision_id', decision_id_value,
        'approved_lifecycle_version', next_lifecycle,
        'approved_payload_version', next_payload_version,
        'approved_payload_hash', next_payload_hash,
        'action_type', proposal_row.action_type,
        'target_ref', proposal_row.target_ref,
        'action_schema_digest', proposal_row.action_schema_digest,
        'action_contract_digest', proposal_row.action_contract_digest,
        'executor_contract_digest', proposal_row.executor_contract_digest,
        'payload_renderer_digest', proposal_row.payload_renderer_digest,
        'governed_tool_binding_digest', proposal_row.governed_tool_binding_digest,
        'provider_connection_contract_digest', proposal_row.provider_connection_contract_digest,
        'provider_capability_digest', proposal_row.provider_capability_digest,
        'provider_operation_id', proposal_row.provider_operation_id,
        'provider_operation_descriptor_digest', proposal_row.provider_operation_descriptor_digest,
        'policy_digest', p_command ->> 'policy_digest',
        'execute_before', to_char(deadline_value AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
      )
    );
    IF p_command ->> 'decision_binding_hash' IS NOT NULL
       AND p_command ->> 'decision_binding_hash' IS DISTINCT FROM decision_binding_value THEN
      RAISE EXCEPTION 'caller-supplied decision binding hash disagrees with the closed decision binding'
        USING ERRCODE = '22023';
    END IF;
    INSERT INTO okg.external_action_decisions (
      decision_id,deployment_name,proposal_id,verdict,
      expected_lifecycle_version,expected_payload_version,
      approved_lifecycle_version,approved_payload_version,
      approved_payload_hash,action_type,target_ref,action_schema_digest,
      action_contract_digest,executor_contract_digest,payload_renderer_digest,
      governed_tool_binding_digest,provider_connection_contract_digest,
      provider_capability_digest,provider_operation_id,
      provider_operation_descriptor_digest,execution_epoch,effect_instance_key,
      approver_principal,approver_separation_subject,authentication_audit_id,
      nomos_decision_id,policy_digest,execute_before,reason_code,
      decision_binding_hash,request_hash,store_record,created_at
    ) VALUES (
      decision_id_value,bound_deployment,proposal_row.proposal_id,'approved',
      proposal_row.lifecycle_version,proposal_row.payload_version,
      next_lifecycle,next_payload_version,next_payload_hash,
      proposal_row.action_type,proposal_row.target_ref,proposal_row.action_schema_digest,
      proposal_row.action_contract_digest,proposal_row.executor_contract_digest,
      proposal_row.payload_renderer_digest,proposal_row.governed_tool_binding_digest,
      proposal_row.provider_connection_contract_digest,proposal_row.provider_capability_digest,
      proposal_row.provider_operation_id,proposal_row.provider_operation_descriptor_digest,
      proposal_row.execution_epoch,proposal_row.effect_instance_key,
      p_command ->> 'actor_principal',p_command ->> 'actor_separation_subject',
      p_command ->> 'authentication_audit_id',p_command ->> 'nomos_decision_id',
      p_command ->> 'policy_digest',deadline_value,p_command ->> 'reason_code',
      decision_binding_value,p_command ->> 'transition_request_hash',p_command,observed_at
    ) RETURNING * INTO decision_row;
    next_review_state := 'approved'; next_execution_state := 'ready';
  ELSIF operation_value = 'reject' THEN
    decision_id_value := p_command ->> 'decision_id';
    IF decision_id_value IS NULL THEN
      RAISE EXCEPTION 'rejection decision id is required' USING ERRCODE = '22023';
    END IF;
    decision_binding_value := okg.external_action_hash(
      'okg-external-action-decision-binding-v1',
      jsonb_build_object(
        'deployment', bound_deployment,
        'execution_epoch', proposal_row.execution_epoch,
        'effect_instance_key', proposal_row.effect_instance_key,
        'proposal_id', proposal_row.proposal_id,
        'decision_id', decision_id_value,
        'approved_lifecycle_version', next_lifecycle,
        'approved_payload_version', next_payload_version,
        'approved_payload_hash', next_payload_hash,
        'action_type', proposal_row.action_type,
        'target_ref', proposal_row.target_ref,
        'action_schema_digest', proposal_row.action_schema_digest,
        'action_contract_digest', proposal_row.action_contract_digest,
        'executor_contract_digest', proposal_row.executor_contract_digest,
        'payload_renderer_digest', proposal_row.payload_renderer_digest,
        'governed_tool_binding_digest', proposal_row.governed_tool_binding_digest,
        'provider_connection_contract_digest', proposal_row.provider_connection_contract_digest,
        'provider_capability_digest', proposal_row.provider_capability_digest,
        'provider_operation_id', proposal_row.provider_operation_id,
        'provider_operation_descriptor_digest', proposal_row.provider_operation_descriptor_digest,
        'policy_digest', p_command ->> 'policy_digest',
        'execute_before', NULL::TEXT
      )
    );
    IF p_command ->> 'decision_binding_hash' IS NOT NULL
       AND p_command ->> 'decision_binding_hash' IS DISTINCT FROM decision_binding_value THEN
      RAISE EXCEPTION 'caller-supplied decision binding hash disagrees with the closed decision binding'
        USING ERRCODE = '22023';
    END IF;
    INSERT INTO okg.external_action_decisions (
      decision_id,deployment_name,proposal_id,verdict,
      expected_lifecycle_version,expected_payload_version,
      approved_lifecycle_version,approved_payload_version,
      approved_payload_hash,action_type,target_ref,action_schema_digest,
      action_contract_digest,executor_contract_digest,payload_renderer_digest,
      governed_tool_binding_digest,provider_connection_contract_digest,
      provider_capability_digest,provider_operation_id,
      provider_operation_descriptor_digest,execution_epoch,effect_instance_key,
      approver_principal,approver_separation_subject,authentication_audit_id,
      nomos_decision_id,policy_digest,reason_code,decision_binding_hash,
      request_hash,store_record,created_at
    ) VALUES (
      decision_id_value,bound_deployment,proposal_row.proposal_id,'rejected',
      proposal_row.lifecycle_version,proposal_row.payload_version,
      next_lifecycle,next_payload_version,next_payload_hash,
      proposal_row.action_type,proposal_row.target_ref,proposal_row.action_schema_digest,
      proposal_row.action_contract_digest,proposal_row.executor_contract_digest,
      proposal_row.payload_renderer_digest,proposal_row.governed_tool_binding_digest,
      proposal_row.provider_connection_contract_digest,proposal_row.provider_capability_digest,
      proposal_row.provider_operation_id,proposal_row.provider_operation_descriptor_digest,
      proposal_row.execution_epoch,proposal_row.effect_instance_key,
      p_command ->> 'actor_principal',p_command ->> 'actor_separation_subject',
      p_command ->> 'authentication_audit_id',p_command ->> 'nomos_decision_id',
      p_command ->> 'policy_digest',p_command ->> 'reason_code',
      decision_binding_value,p_command ->> 'transition_request_hash',p_command,observed_at
    ) RETURNING * INTO decision_row;
    next_review_state := 'rejected'; next_execution_state := 'unavailable';
  ELSIF operation_value = 'cancel' THEN
    next_review_state := 'cancelled'; next_execution_state := 'unavailable';
  END IF;
  -- Validate the request hash only after the closed decision binding has been
  -- recomputed.  A caller-supplied decision binding therefore receives the
  -- typed 22023 refusal before a stale/forged request hash can mask it.
  IF proposal_row.action_contract_record ? 'outcome_reason_rules' THEN
    canonical_transition_request_hash:=okg.external_action_hash(
      'okg-external-action-transition-request-v1',jsonb_build_object(
        'operation',operation_value,
        'action_contract_digest',COALESCE(p_command->>'action_contract_digest',proposal_row.action_contract_digest),
        'expected_lifecycle_version',NULLIF(p_command->>'expected_lifecycle_version','')::bigint,
        'expected_payload_version',NULLIF(p_command->>'expected_payload_version','')::bigint,
        'expected_payload_hash',p_command->>'expected_payload_hash',
        'expected_decision_id',p_command->>'expected_decision_id',
        'expected_decision_binding_hash',p_command->>'expected_decision_binding_hash',
        'replacement_payload_hash',canonical_replacement_payload_hash,
        'execute_before',CASE WHEN p_command->>'execute_before' IS NULL THEN NULL ELSE to_char((p_command->>'execute_before')::timestamptz AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"') END,
        'reason_code',p_command->>'reason_code'));
    IF p_command->>'transition_request_hash' IS DISTINCT FROM canonical_transition_request_hash THEN
      RAISE EXCEPTION 'transition request hash is not canonical' USING ERRCODE='40001';
    END IF;
  END IF;
  UPDATE okg.external_action_proposals
     SET review_state = next_review_state, execution_state = next_execution_state,
         lifecycle_version = next_lifecycle, payload_version = next_payload_version,
         current_payload_hash = next_payload_hash,
         current_decision_id = CASE WHEN operation_value IN ('approve','approve_with_edit','reject') THEN decision_id_value ELSE current_decision_id END,
         current_decision_binding_hash = CASE WHEN operation_value IN ('approve','approve_with_edit','reject') THEN decision_binding_value ELSE current_decision_binding_hash END,
         execute_before = CASE WHEN operation_value IN ('approve','approve_with_edit') THEN deadline_value ELSE execute_before END,
         updated_at = observed_at
   WHERE deployment_name = bound_deployment AND proposal_id = proposal_row.proposal_id
  RETURNING * INTO proposal_row;
  SELECT COALESCE(max(sequence),0) + 1,
         (array_agg(event_hash ORDER BY sequence))[array_length(array_agg(event_hash ORDER BY sequence),1)]
    INTO sequence_value, previous_event_hash_value
    FROM okg.external_action_events
   WHERE deployment_name = bound_deployment AND effect_instance_key = proposal_row.effect_instance_key;
  IF p_command ->> 'previous_event_hash' IS NOT NULL AND p_command ->> 'previous_event_hash' IS DISTINCT FROM previous_event_hash_value THEN
    RAISE EXCEPTION 'external action event chain predecessor is stale' USING ERRCODE = '40001';
  END IF;
  event_projection := jsonb_build_object(
    'proposal_id', proposal_row.proposal_id,
    'execution_epoch', proposal_row.execution_epoch,
    'effect_instance_key', proposal_row.effect_instance_key,
    'sequence', sequence_value,
    'lifecycle_version', proposal_row.lifecycle_version,
    'event_type', CASE operation_value WHEN 'edit' THEN 'edited' WHEN 'defer' THEN 'deferred' WHEN 'approve' THEN 'approved' WHEN 'approve_with_edit' THEN 'approved' WHEN 'reject' THEN 'rejected' ELSE 'cancelled' END,
    'actor_kind', 'authenticated_principal',
    'actor_principal', p_command ->> 'actor_principal',
    'actor_separation_subject', p_command ->> 'actor_separation_subject',
    'authentication_audit_ref', p_command ->> 'authentication_audit_id',
    'nomos_decision_ref', p_command ->> 'nomos_decision_id',
    'deadline_kind', NULL::TEXT,
    'policy_digest', p_command ->> 'policy_digest',
    'before_review_state', previous_proposal.review_state,
    'after_review_state', proposal_row.review_state,
    'before_execution_state', previous_proposal.execution_state,
    'after_execution_state', proposal_row.execution_state,
    'payload_version', proposal_row.payload_version,
    'payload_hash', proposal_row.current_payload_hash,
    'decision_id', proposal_row.current_decision_id,
    'decision_binding_hash', proposal_row.current_decision_binding_hash,
    'execution_id', NULL::TEXT,
    'execution_binding_hash', NULL::TEXT,
    'transition_reason_code', p_command ->> 'reason_code',
    'outcome', NULL::TEXT,
    'outcome_reason_code', NULL::TEXT,
    'outcome_request_hash', NULL::TEXT,
    'restore_recovery_digest', NULL::TEXT,
    'effective_at', NULL::TEXT,
    'recorded_at', to_char(observed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
    'previous_event_hash', previous_event_hash_value
  );
  event_hash_value := okg.external_action_hash('okg-external-action-event-v1', event_projection);
  IF p_command ->> 'event_hash' IS NOT NULL
     AND p_command ->> 'event_hash' IS DISTINCT FROM event_hash_value THEN
    RAISE EXCEPTION 'caller-supplied event hash disagrees with the closed event projection'
      USING ERRCODE = '22023';
  END IF;
  INSERT INTO okg.external_action_events (
    deployment_name,proposal_id,execution_epoch,effect_instance_key,sequence,lifecycle_version,
    event_type,actor_kind,actor_principal,actor_separation_subject,
    authentication_audit_ref,nomos_decision_ref,policy_digest,
    before_review_state,after_review_state,before_execution_state,after_execution_state,
    payload_version,payload_hash,decision_id,decision_binding_hash,
    effective_at,recorded_at,previous_event_hash,event_hash,store_record
  ) VALUES (
    bound_deployment,proposal_row.proposal_id,proposal_row.execution_epoch,
    proposal_row.effect_instance_key,sequence_value,proposal_row.lifecycle_version,
    CASE operation_value WHEN 'edit' THEN 'edited' WHEN 'defer' THEN 'deferred'
      WHEN 'approve' THEN 'approved' WHEN 'approve_with_edit' THEN 'approved'
      WHEN 'reject' THEN 'rejected' ELSE 'cancelled' END,
    'authenticated_principal',p_command ->> 'actor_principal',
    p_command ->> 'actor_separation_subject',p_command ->> 'authentication_audit_id',
    p_command ->> 'nomos_decision_id',p_command ->> 'policy_digest',
    previous_proposal.review_state,proposal_row.review_state,
    previous_proposal.execution_state,proposal_row.execution_state,
    proposal_row.payload_version,proposal_row.current_payload_hash,
    proposal_row.current_decision_id,proposal_row.current_decision_binding_hash,
    NULL,observed_at,previous_event_hash_value,event_hash_value,
    jsonb_build_object('event_projection', event_projection)
  );
  response_body_value := COALESCE(okg.external_action_json_bytes(p_command -> 'response_body_jcs'), convert_to(jsonb_build_object('proposal_id',proposal_row.proposal_id,'deployment',bound_deployment,'review_state',proposal_row.review_state,'execution_state',proposal_row.execution_state,'lifecycle_version',proposal_row.lifecycle_version,'payload_version',proposal_row.payload_version,'current_payload_hash',proposal_row.current_payload_hash)::text,'utf8'));
  INSERT INTO okg.external_action_transition_idempotency (
    deployment_name,proposal_id,operation,actor_separation_subject,idempotency_key_hash,
    transition_request_hash,response_status,response_media_type,response_body_jcs,
    result_event_ids,result_payload_version,result_decision_id,result_lifecycle_version,
    result_etag,authentication_audit_id,nomos_decision_id,store_record,committed_at
  ) VALUES (
    bound_deployment,proposal_row.proposal_id,operation_value,p_command ->> 'actor_separation_subject',
    p_command ->> 'idempotency_key_hash',p_command ->> 'transition_request_hash',
    COALESCE((p_command ->> 'response_status')::integer,200),
    COALESCE(p_command ->> 'response_media_type','application/json'),response_body_value,
    COALESCE(p_command -> 'result_event_ids','[]'::jsonb),
    NULLIF(p_command ->> 'result_payload_version','')::bigint,
    NULLIF(p_command ->> 'result_decision_id',''),
    NULLIF(p_command ->> 'result_lifecycle_version','')::bigint,p_command ->> 'result_etag',
    p_command ->> 'authentication_audit_id',p_command ->> 'nomos_decision_id',
    jsonb_build_object('command',p_command,'response_projection',jsonb_build_object('replay',FALSE,'proposal_id',proposal_row.proposal_id,'deployment',bound_deployment,'review_state',proposal_row.review_state,'execution_state',proposal_row.execution_state,'lifecycle_version',proposal_row.lifecycle_version,'payload_version',proposal_row.payload_version,'current_payload_hash',proposal_row.current_payload_hash,'current_decision_id',proposal_row.current_decision_id,'current_decision_binding_hash',proposal_row.current_decision_binding_hash)),observed_at
  );
  RETURN jsonb_build_object('replay',FALSE,'proposal_id',proposal_row.proposal_id,'deployment',bound_deployment,'review_state',proposal_row.review_state,'execution_state',proposal_row.execution_state,'lifecycle_version',proposal_row.lifecycle_version,'payload_version',proposal_row.payload_version,'current_payload_hash',proposal_row.current_payload_hash,'current_decision_id',proposal_row.current_decision_id,'current_decision_binding_hash',proposal_row.current_decision_binding_hash,'event_hash',event_hash_value);
END
$external_action_review_transition$;

-- Prepare the claim handoff while holding the same deployment fence used by
-- the mutating claim RPC.  This is read-only: the service signs the returned
-- closed handoff fields, then calls external_action_claim in this transaction.
CREATE OR REPLACE FUNCTION okg.external_action_prepare_claim(p_command JSONB)
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $external_action_prepare_claim$
DECLARE
  bound_deployment TEXT := okg.external_action_session_deployment();
  proposal_row okg.external_action_proposals%ROWTYPE;
  decision_row okg.external_action_decisions%ROWTYPE;
  existing_execution okg.external_action_executions%ROWTYPE;
  payload_row okg.external_action_payload_versions%ROWTYPE;
  lease_digest_value TEXT;
  previous_event_hash_value TEXT;
  decision_event_hash_value TEXT;
  claim_event_hash_value TEXT;
  event_projection JSONB;
  observed_at TIMESTAMPTZ;
  event_sequence BIGINT;
  claim_lifecycle_version BIGINT;
BEGIN
  PERFORM okg.external_action_verifier_capability_status();
  PERFORM okg.external_action_require_command(
    p_command,
    ARRAY[
      'deployment','proposal_id','execution_id','decision_id',
      'worker_principal','worker_separation_subject',
      'approved_lifecycle_version','approved_payload_version',
      'approved_payload_hash','execution_epoch_lease_digest',
      'execution_binding_hash','request_hash',
      'authentication_audit_id',
      'nomos_decision_id','policy_digest','event_hash','previous_event_hash'
    ],
    ARRAY[
      'deployment','proposal_id','execution_id','decision_id',
      'worker_principal','worker_separation_subject',
      'approved_lifecycle_version','approved_payload_version',
      'approved_payload_hash','execution_epoch_lease_digest',
      'execution_binding_hash','request_hash',
      'authentication_audit_id',
      'nomos_decision_id','policy_digest'
    ]
  );
  IF p_command ->> 'deployment' <> bound_deployment THEN
    RAISE EXCEPTION 'external action deployment is not bound to session_user'
      USING ERRCODE='28000';
  END IF;
  PERFORM pg_advisory_xact_lock(
    hashtextextended('okg.external_action.deployment:' || bound_deployment,0)
  );
  PERFORM okg.external_action_nomos_authority_snapshot(
    bound_deployment,p_command ->> 'authentication_audit_id',
    p_command ->> 'nomos_decision_id','external_action.execute',
    p_command ->> 'request_hash'
  );
  SELECT * INTO proposal_row
    FROM okg.external_action_proposals
   WHERE deployment_name = bound_deployment
     AND proposal_id = p_command ->> 'proposal_id'
   FOR SHARE;
  IF proposal_row.proposal_id IS NULL THEN
    RAISE EXCEPTION 'external action proposal was not found' USING ERRCODE='P0002';
  END IF;
  SELECT * INTO existing_execution
    FROM okg.external_action_executions
   WHERE deployment_name = bound_deployment AND proposal_id = proposal_row.proposal_id
   FOR SHARE;
  IF existing_execution.execution_id IS NOT NULL THEN
    RAISE EXCEPTION 'external action proposal has already been claimed'
      USING ERRCODE='40001';
  END IF;
  IF proposal_row.review_state <> 'approved'
     OR proposal_row.execution_state <> 'ready' THEN
    RAISE EXCEPTION 'external action proposal is not ready to claim'
      USING ERRCODE='40001';
  END IF;
  observed_at := clock_timestamp();
  IF proposal_row.execute_before IS NULL OR observed_at >= proposal_row.execute_before THEN
    RAISE EXCEPTION 'external action execution deadline has passed' USING ERRCODE='22008';
  END IF;
  SELECT * INTO decision_row
    FROM okg.external_action_decisions
   WHERE deployment_name = bound_deployment
     AND decision_id = p_command ->> 'decision_id'
     AND proposal_id = proposal_row.proposal_id
   FOR SHARE;
  IF decision_row.decision_id IS NULL
     OR decision_row.verdict <> 'approved'
     OR decision_row.decision_binding_hash <> proposal_row.current_decision_binding_hash THEN
    RAISE EXCEPTION 'external action approval decision is unavailable' USING ERRCODE='40001';
  END IF;
  IF p_command->>'policy_digest' IS DISTINCT FROM decision_row.policy_digest THEN
    RAISE EXCEPTION 'external action policy binding is stale' USING ERRCODE='40001';
  END IF;
  IF (p_command ->> 'approved_lifecycle_version')::bigint <> proposal_row.lifecycle_version
     OR (p_command ->> 'approved_payload_version')::bigint <> proposal_row.payload_version
     OR p_command ->> 'approved_payload_hash' <> proposal_row.current_payload_hash THEN
    RAISE EXCEPTION 'external action approval payload binding is stale' USING ERRCODE='40001';
  END IF;
  SELECT envelope_digest INTO lease_digest_value
    FROM okg.external_action_execution_epoch_leases
   WHERE deployment_name = bound_deployment AND epoch = proposal_row.execution_epoch
   ORDER BY lease_sequence DESC LIMIT 1;
  IF lease_digest_value IS NULL
     OR lease_digest_value <> p_command ->> 'execution_epoch_lease_digest' THEN
    RAISE EXCEPTION 'external action execution epoch lease is stale' USING ERRCODE='40001';
  END IF;
  SELECT * INTO payload_row
    FROM okg.external_action_payload_versions
   WHERE deployment_name = bound_deployment
     AND proposal_id = proposal_row.proposal_id
     AND payload_version = proposal_row.payload_version
   FOR SHARE;
  IF payload_row.payload_version IS NULL THEN
    RAISE EXCEPTION 'external action payload is unavailable' USING ERRCODE='40001';
  END IF;
  SELECT COALESCE(max(sequence),0) + 1,
         (array_agg(event_hash ORDER BY sequence))[array_length(array_agg(event_hash ORDER BY sequence),1)]
    INTO event_sequence, previous_event_hash_value
    FROM okg.external_action_events
   WHERE deployment_name = bound_deployment
     AND effect_instance_key = proposal_row.effect_instance_key;
  SELECT event_hash INTO decision_event_hash_value
    FROM okg.external_action_events
   WHERE deployment_name = bound_deployment
     AND effect_instance_key = proposal_row.effect_instance_key
     AND event_type = 'approved'
     AND decision_id = decision_row.decision_id
   ORDER BY sequence DESC LIMIT 1;
  IF decision_event_hash_value IS NULL THEN
    RAISE EXCEPTION 'external action approval event is unavailable' USING ERRCODE='40001';
  END IF;
  claim_lifecycle_version := proposal_row.lifecycle_version + 1;
  event_projection := jsonb_build_object(
    'proposal_id', proposal_row.proposal_id,
    'execution_epoch', proposal_row.execution_epoch,
    'effect_instance_key', proposal_row.effect_instance_key,
    'sequence', event_sequence,
    'lifecycle_version', claim_lifecycle_version,
    'event_type', 'execution_claimed',
    'actor_kind', 'authenticated_principal',
    'actor_principal', p_command ->> 'worker_principal',
    'actor_separation_subject', p_command ->> 'worker_separation_subject',
    'authentication_audit_ref', p_command ->> 'authentication_audit_id',
    'nomos_decision_ref', p_command ->> 'nomos_decision_id',
    'deadline_kind', NULL::TEXT,
    'policy_digest', decision_row.policy_digest,
    'before_review_state', 'approved', 'after_review_state', 'approved',
    'before_execution_state', 'ready', 'after_execution_state', 'claimed',
    'payload_version', proposal_row.payload_version,
    'payload_hash', proposal_row.current_payload_hash,
    'decision_id', decision_row.decision_id,
    'decision_binding_hash', decision_row.decision_binding_hash,
    'execution_id', p_command ->> 'execution_id',
    'execution_binding_hash', p_command ->> 'execution_binding_hash',
    'transition_reason_code', NULL::TEXT, 'outcome', NULL::TEXT,
    'outcome_reason_code', NULL::TEXT, 'outcome_request_hash', NULL::TEXT,
    'restore_recovery_digest', NULL::TEXT, 'effective_at', NULL::TEXT,
    'recorded_at', to_char(observed_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
    'previous_event_hash', previous_event_hash_value
  );
  claim_event_hash_value := okg.external_action_hash(
    'okg-external-action-event-v1', event_projection
  );
  IF p_command ->> 'event_hash' IS NOT NULL
     AND p_command ->> 'event_hash' IS DISTINCT FROM claim_event_hash_value THEN
    RAISE EXCEPTION 'caller-supplied event hash disagrees with the prepared claim event'
      USING ERRCODE='22023';
  END IF;
  IF p_command ->> 'previous_event_hash' IS NOT NULL
     AND p_command ->> 'previous_event_hash' IS DISTINCT FROM previous_event_hash_value THEN
    RAISE EXCEPTION 'prepared claim event predecessor is stale' USING ERRCODE='40001';
  END IF;
  RETURN jsonb_build_object(
    'claimed_at', to_char(observed_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
    'previous_event_hash', previous_event_hash_value,
    'decision_event_hash', decision_event_hash_value,
    'claim_event_hash', claim_event_hash_value,
    'event_chain_head_hash', claim_event_hash_value,
    'claim_lifecycle_version', claim_lifecycle_version,
    'event_sequence', event_sequence
  );
END
$external_action_prepare_claim$;

CREATE OR REPLACE FUNCTION okg.external_action_claim(p_command JSONB)
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, okg AS $external_action_claim$
DECLARE
  bound_deployment TEXT := okg.external_action_session_deployment();
  proposal_row okg.external_action_proposals%ROWTYPE;
  execution_row okg.external_action_executions%ROWTYPE;
  existing_execution okg.external_action_executions%ROWTYPE;
  fence_row okg.external_action_restore_fences%ROWTYPE;
  decision_row okg.external_action_decisions%ROWTYPE;
  payload_row okg.external_action_payload_versions%ROWTYPE;
  existing_event okg.external_action_events%ROWTYPE;
  lease_digest_value TEXT;
  observed_at TIMESTAMPTZ := clock_timestamp();
  claimed_at_value TIMESTAMPTZ;
  decision_event_hash_value TEXT;
  claim_event_hash_value TEXT;
  prepared_claim_lifecycle_version BIGINT;
  prepared_event_sequence BIGINT;
  execution_binding_projection JSONB;
  provider_idempotency_projection JSONB;
  provider_idempotency_key_value TEXT;
  provider_idempotency_digest_value TEXT;
  event_sequence BIGINT;
  previous_event_hash_value TEXT;
  event_hash_value TEXT;
  event_projection JSONB;
  canonical_claim_request_hash TEXT;
BEGIN
  PERFORM okg.external_action_verifier_capability_status();
  PERFORM okg.external_action_require_command(
    p_command,
    ARRAY[
      'deployment','proposal_id','execution_id','decision_id',
      'worker_principal','worker_separation_subject','idempotency_key_hash',
      'request_hash','approved_lifecycle_version','approved_payload_version',
      'approved_payload_hash','execution_epoch_lease_digest',
      'supported_action_schema_digest','supported_action_contract_digest',
      'supported_executor_contract_digest','supported_payload_renderer_digest',
      'supported_governed_tool_binding_digest',
      'supported_provider_connection_contract_digest',
      'supported_provider_capability_digest','supported_provider_operation_id',
      'supported_provider_operation_descriptor_digest',
      'execution_binding_hash','provider_idempotency_key',
      'provider_idempotency_key_digest',
      'okg_claim_handoff_receipt_digest','handoff_protected_b64',
      'handoff_payload_b64','handoff_signature_b64','authentication_audit_id',
      'nomos_decision_id','policy_digest','event_hash','previous_event_hash',
      'claimed_at','decision_event_hash','claim_event_hash',
      'event_chain_head_hash','claim_lifecycle_version','event_sequence','if_match'
    ],
    ARRAY[
      'deployment','proposal_id','execution_id','decision_id',
      'worker_principal','worker_separation_subject','idempotency_key_hash',
      'request_hash','approved_lifecycle_version','approved_payload_version',
      'approved_payload_hash','execution_epoch_lease_digest',
      'supported_action_schema_digest','supported_action_contract_digest',
      'supported_executor_contract_digest','supported_payload_renderer_digest',
      'supported_governed_tool_binding_digest',
      'supported_provider_connection_contract_digest',
      'supported_provider_capability_digest','supported_provider_operation_id',
      'supported_provider_operation_descriptor_digest',
      'execution_binding_hash','provider_idempotency_key',
      'provider_idempotency_key_digest',
      'okg_claim_handoff_receipt_digest','handoff_protected_b64',
      'handoff_payload_b64','handoff_signature_b64','authentication_audit_id',
      'nomos_decision_id','event_hash','previous_event_hash','claimed_at',
      'decision_event_hash','claim_event_hash','event_chain_head_hash',
      'claim_lifecycle_version','event_sequence','if_match'
    ]
  );
  IF p_command ->> 'deployment' <> bound_deployment THEN RAISE EXCEPTION 'external action deployment is not bound to session_user' USING ERRCODE='28000'; END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended('okg.external_action.deployment:' || bound_deployment, 0));
  PERFORM okg.external_action_nomos_authority_snapshot(bound_deployment,p_command->>'authentication_audit_id',p_command->>'nomos_decision_id','external_action.execute',p_command->>'request_hash');
  SELECT * INTO proposal_row FROM okg.external_action_proposals WHERE deployment_name=bound_deployment AND proposal_id=p_command->>'proposal_id' FOR UPDATE;
  IF proposal_row.proposal_id IS NULL THEN RAISE EXCEPTION 'external action proposal was not found' USING ERRCODE='P0002'; END IF;
  -- The worker request hash is over the approved fields plus the exact
  -- retained action binding.  A direct mapped-login caller cannot replace
  -- those support digests or replay an invented claim.
  IF NOT (proposal_row.action_contract_record ? 'outcome_reason_rules') THEN
    RAISE EXCEPTION 'external action contract authority is unavailable'
      USING ERRCODE='40001';
  END IF;
  IF p_command->>'supported_action_schema_digest' IS DISTINCT FROM proposal_row.action_schema_digest
     OR p_command->>'supported_action_contract_digest' IS DISTINCT FROM proposal_row.action_contract_digest
     OR p_command->>'supported_executor_contract_digest' IS DISTINCT FROM proposal_row.executor_contract_digest
     OR p_command->>'supported_payload_renderer_digest' IS DISTINCT FROM proposal_row.payload_renderer_digest
     OR p_command->>'supported_governed_tool_binding_digest' IS DISTINCT FROM proposal_row.governed_tool_binding_digest
     OR p_command->>'supported_provider_connection_contract_digest' IS DISTINCT FROM proposal_row.provider_connection_contract_digest
     OR p_command->>'supported_provider_capability_digest' IS DISTINCT FROM proposal_row.provider_capability_digest
     OR p_command->>'supported_provider_operation_id' IS DISTINCT FROM proposal_row.provider_operation_id
     OR p_command->>'supported_provider_operation_descriptor_digest' IS DISTINCT FROM proposal_row.provider_operation_descriptor_digest THEN
    RAISE EXCEPTION 'worker effect binding is not supported' USING ERRCODE='40001';
  END IF;
    canonical_claim_request_hash:=okg.external_action_hash(
      'okg-external-action-transition-request-v1',jsonb_build_object(
        'worker_separation_subject',p_command->>'worker_separation_subject',
        'decision_id',p_command->>'decision_id',
        'approved_lifecycle_version',(p_command->>'approved_lifecycle_version')::bigint,
        'approved_payload_version',(p_command->>'approved_payload_version')::bigint,
        'approved_payload_hash',p_command->>'approved_payload_hash',
        'supported_action_schema_digest',p_command->>'supported_action_schema_digest',
        'supported_action_contract_digest',p_command->>'supported_action_contract_digest',
        'supported_executor_contract_digest',p_command->>'supported_executor_contract_digest',
        'supported_payload_renderer_digest',p_command->>'supported_payload_renderer_digest',
        'supported_governed_tool_binding_digest',p_command->>'supported_governed_tool_binding_digest',
        'supported_provider_connection_contract_digest',p_command->>'supported_provider_connection_contract_digest',
        'supported_provider_capability_digest',p_command->>'supported_provider_capability_digest',
        'supported_provider_operation_id',p_command->>'supported_provider_operation_id',
        'supported_provider_operation_descriptor_digest',p_command->>'supported_provider_operation_descriptor_digest',
        'if_match',p_command->>'if_match'));
    IF p_command->>'if_match' IS DISTINCT FROM format(
         '"lifecycle:%s;payload-version:%s;payload:%s"',
         p_command->>'approved_lifecycle_version',
         p_command->>'approved_payload_version',
         p_command->>'approved_payload_hash'
       )
       OR p_command->>'request_hash' IS DISTINCT FROM canonical_claim_request_hash THEN
      RAISE EXCEPTION 'claim request hash is not canonical' USING ERRCODE='40001';
    END IF;
  SELECT * INTO existing_execution FROM okg.external_action_executions WHERE deployment_name=bound_deployment AND proposal_id=proposal_row.proposal_id FOR UPDATE;
  IF existing_execution.execution_id IS NOT NULL THEN
    IF existing_execution.store_record->>'idempotency_key_hash'=p_command->>'idempotency_key_hash' AND existing_execution.request_hash=p_command->>'request_hash' THEN
      SELECT * INTO existing_event FROM okg.external_action_events WHERE deployment_name=bound_deployment AND execution_id=existing_execution.execution_id AND event_type='execution_claimed' ORDER BY sequence DESC LIMIT 1;
      IF existing_event.event_id IS NULL OR p_command->>'event_hash' IS DISTINCT FROM existing_event.event_hash OR p_command->>'previous_event_hash' IS DISTINCT FROM existing_event.previous_event_hash OR (p_command->>'claim_lifecycle_version')::bigint<>existing_event.lifecycle_version OR (p_command->>'event_sequence')::bigint<>existing_event.sequence OR p_command->>'claim_event_hash' IS DISTINCT FROM existing_event.event_hash OR p_command->>'event_chain_head_hash' IS DISTINCT FROM existing_event.event_hash OR p_command->>'claimed_at' IS DISTINCT FROM to_char(existing_event.recorded_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"') THEN
        RAISE EXCEPTION 'prepared claim replay does not match the retained claim event' USING ERRCODE='40001';
      END IF;
      RETURN jsonb_build_object(
        'replay',TRUE,'deployment',bound_deployment,
        'proposal_id',existing_execution.proposal_id,
        'execution_id',existing_execution.execution_id,
        'decision_id',existing_execution.decision_id,
        'execution_state','claimed',
        'execution_binding_hash',existing_execution.execution_binding_hash,
        'provider_idempotency_key',existing_execution.store_record->>'provider_idempotency_key',
        'provider_idempotency_key_digest',existing_execution.provider_idempotency_key_digest,
        'okg_claim_handoff_receipt_digest',existing_execution.okg_claim_handoff_receipt_digest,
        'handoff_protected_b64',existing_execution.handoff_protected_b64,
        'handoff_payload_b64',existing_execution.handoff_payload_b64,
        'handoff_signature_b64',existing_execution.handoff_signature_b64,
        'canonical_payload',jsonb_build_object(
          '__okg_bytes_b64',replace(
            encode(existing_execution.canonical_payload,'base64'),E'\n',''
          )
        )
      );
    END IF;
    RAISE EXCEPTION 'external action proposal has already been claimed' USING ERRCODE='40001';
  END IF;
  -- Claiming mutates the proposal and creates an execution. Enforce the
  -- persisted startup fence before either mutation; the insert trigger is a
  -- second, row-level guard for the execution insert.
  SELECT * INTO fence_row
    FROM okg.external_action_restore_fences
   WHERE deployment_name = bound_deployment
   FOR SHARE;
  IF fence_row.deployment_name IS NOT NULL
     AND (fence_row.service_mode <> 'normal'
          OR fence_row.quarantine_state <> 'clear') THEN
    RAISE EXCEPTION 'external action lifecycle is quarantined for restore'
      USING ERRCODE='40001';
  END IF;
  IF fence_row.deployment_name IS NOT NULL
     AND proposal_row.execution_epoch < fence_row.highest_accepted_epoch THEN
    RAISE EXCEPTION 'external action lifecycle epoch is stale'
      USING ERRCODE='40001';
  END IF;
  claimed_at_value := (p_command->>'claimed_at')::timestamptz;
  observed_at := claimed_at_value;
  prepared_claim_lifecycle_version := (p_command->>'claim_lifecycle_version')::bigint;
  prepared_event_sequence := (p_command->>'event_sequence')::bigint;
  IF p_command->>'claimed_at' !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z$' THEN RAISE EXCEPTION 'prepared claim timestamp is not canonical UTC' USING ERRCODE='22023'; END IF;
  IF proposal_row.review_state<>'approved' OR proposal_row.execution_state<>'ready' THEN RAISE EXCEPTION 'external action proposal is not ready to claim' USING ERRCODE='40001'; END IF;
  IF proposal_row.current_decision_id<>p_command->>'decision_id' OR proposal_row.current_decision_binding_hash IS NULL THEN RAISE EXCEPTION 'external action approval binding is stale' USING ERRCODE='40001'; END IF;
  SELECT * INTO decision_row FROM okg.external_action_decisions WHERE deployment_name=bound_deployment AND decision_id=p_command->>'decision_id' AND proposal_id=proposal_row.proposal_id FOR SHARE;
  IF decision_row.decision_id IS NULL OR decision_row.verdict<>'approved' OR decision_row.decision_binding_hash<>proposal_row.current_decision_binding_hash THEN RAISE EXCEPTION 'external action approval decision is unavailable' USING ERRCODE='40001'; END IF;
  IF p_command->>'policy_digest' IS DISTINCT FROM decision_row.policy_digest THEN RAISE EXCEPTION 'external action policy binding is stale' USING ERRCODE='40001'; END IF;
  IF (p_command->>'approved_lifecycle_version')::bigint<>proposal_row.lifecycle_version OR (p_command->>'approved_payload_version')::bigint<>proposal_row.payload_version OR p_command->>'approved_payload_hash'<>proposal_row.current_payload_hash THEN RAISE EXCEPTION 'external action approval payload binding is stale' USING ERRCODE='40001'; END IF;
  IF proposal_row.execute_before IS NULL OR clock_timestamp()>=proposal_row.execute_before OR claimed_at_value>=proposal_row.execute_before THEN RAISE EXCEPTION 'external action execution deadline has passed' USING ERRCODE='22008'; END IF;
  SELECT envelope_digest INTO lease_digest_value FROM okg.external_action_execution_epoch_leases WHERE deployment_name=bound_deployment AND epoch=proposal_row.execution_epoch ORDER BY lease_sequence DESC LIMIT 1;
  IF lease_digest_value IS NULL OR lease_digest_value<>p_command->>'execution_epoch_lease_digest' THEN RAISE EXCEPTION 'external action execution epoch lease is stale' USING ERRCODE='40001'; END IF;
  execution_binding_projection := jsonb_build_object(
    'deployment',bound_deployment,'execution_epoch',proposal_row.execution_epoch,
    'execution_epoch_lease_digest',lease_digest_value,
    'effect_instance_key',proposal_row.effect_instance_key,
    'proposal_id',proposal_row.proposal_id,'decision_id',decision_row.decision_id,
    'execution_id',p_command->>'execution_id',
    'worker_separation_subject',p_command->>'worker_separation_subject',
    'approved_lifecycle_version',proposal_row.lifecycle_version,
    'approved_payload_version',proposal_row.payload_version,
    'approved_payload_hash',proposal_row.current_payload_hash,
    'action_type',proposal_row.action_type,'target_ref',proposal_row.target_ref,
    'action_schema_digest',proposal_row.action_schema_digest,
    'action_contract_digest',proposal_row.action_contract_digest,
    'executor_contract_digest',proposal_row.executor_contract_digest,
    'payload_renderer_digest',proposal_row.payload_renderer_digest,
    'governed_tool_binding_digest',proposal_row.governed_tool_binding_digest,
    'provider_connection_contract_digest',proposal_row.provider_connection_contract_digest,
    'provider_capability_digest',proposal_row.provider_capability_digest,
    'provider_operation_id',proposal_row.provider_operation_id,
    'provider_operation_descriptor_digest',proposal_row.provider_operation_descriptor_digest,
    'policy_digest',decision_row.policy_digest
  );
  provider_idempotency_projection := jsonb_build_object(
    'deployment',bound_deployment,'effect_instance_key',proposal_row.effect_instance_key,
    'approved_payload_hash',proposal_row.current_payload_hash,
    'action_contract_digest',proposal_row.action_contract_digest,
    'provider_connection_contract_digest',proposal_row.provider_connection_contract_digest,
    'provider_operation_id',proposal_row.provider_operation_id,
    'provider_operation_descriptor_digest',proposal_row.provider_operation_descriptor_digest
  );
  provider_idempotency_key_value := okg.external_action_effect_key(provider_idempotency_projection);
  provider_idempotency_digest_value := okg.external_action_hash('okg-external-action-effect-v1',provider_idempotency_projection);
  IF p_command->>'execution_binding_hash' IS DISTINCT FROM okg.external_action_hash('okg-external-action-execution-binding-v1',execution_binding_projection)
     OR p_command->>'provider_idempotency_key' IS DISTINCT FROM provider_idempotency_key_value
     OR p_command->>'provider_idempotency_key_digest' IS DISTINCT FROM provider_idempotency_digest_value THEN
    RAISE EXCEPTION 'caller-supplied execution or provider binding disagrees with the closed projection' USING ERRCODE='22023';
  END IF;
  IF p_command->>'okg_claim_handoff_receipt_digest' IS DISTINCT FROM
       okg.external_action_hash(
         'okg-external-action-claim-handoff-receipt-envelope-v1',
         jsonb_build_object(
           'protected_b64',p_command->>'handoff_protected_b64',
           'payload_b64',p_command->>'handoff_payload_b64',
           'signature_b64',p_command->>'handoff_signature_b64'
         )
       ) THEN
    RAISE EXCEPTION 'claim handoff envelope digest is not canonical' USING ERRCODE='22023';
  END IF;
  SELECT * INTO payload_row FROM okg.external_action_payload_versions WHERE deployment_name=bound_deployment AND proposal_id=proposal_row.proposal_id AND payload_version=proposal_row.payload_version FOR SHARE;
  IF payload_row.payload_version IS NULL THEN RAISE EXCEPTION 'external action payload is unavailable' USING ERRCODE='40001'; END IF;
  SELECT COALESCE(max(sequence),0)+1,(array_agg(event_hash ORDER BY sequence))[array_length(array_agg(event_hash ORDER BY sequence),1)] INTO event_sequence,previous_event_hash_value FROM okg.external_action_events WHERE deployment_name=bound_deployment AND effect_instance_key=proposal_row.effect_instance_key;
  SELECT event_hash INTO decision_event_hash_value FROM okg.external_action_events WHERE deployment_name=bound_deployment AND effect_instance_key=proposal_row.effect_instance_key AND event_type='approved' AND decision_id=decision_row.decision_id ORDER BY sequence DESC LIMIT 1;
  IF decision_event_hash_value IS NULL THEN RAISE EXCEPTION 'external action approval event is unavailable' USING ERRCODE='40001'; END IF;
  IF prepared_event_sequence<>event_sequence OR p_command->>'previous_event_hash' IS DISTINCT FROM previous_event_hash_value OR p_command->>'decision_event_hash' IS DISTINCT FROM decision_event_hash_value OR prepared_claim_lifecycle_version<>proposal_row.lifecycle_version+1 THEN RAISE EXCEPTION 'prepared claim event is stale' USING ERRCODE='40001'; END IF;
  INSERT INTO okg.external_action_executions (
    execution_id,deployment_name,proposal_id,decision_id,execution_epoch,execution_epoch_lease_digest,
    effect_instance_key,worker_separation_subject,approved_lifecycle_version,approved_payload_version,
    approved_payload_hash,action_type,target_ref,action_schema_digest,action_contract_digest,
    executor_contract_digest,payload_renderer_digest,governed_tool_binding_digest,
    provider_connection_contract_digest,provider_capability_digest,provider_operation_id,
    provider_operation_descriptor_digest,policy_digest,execution_binding_hash,request_hash,
    okg_claim_handoff_receipt_digest,handoff_protected_b64,handoff_payload_b64,handoff_signature_b64,
    canonical_payload,provider_idempotency_key_digest,authentication_audit_id,nomos_decision_id,
    claimed_at,execute_before,store_record
  ) VALUES (
    p_command->>'execution_id',bound_deployment,proposal_row.proposal_id,decision_row.decision_id,
    proposal_row.execution_epoch,lease_digest_value,proposal_row.effect_instance_key,
    p_command->>'worker_separation_subject',proposal_row.lifecycle_version,proposal_row.payload_version,
    proposal_row.current_payload_hash,proposal_row.action_type,proposal_row.target_ref,
    proposal_row.action_schema_digest,proposal_row.action_contract_digest,
    proposal_row.executor_contract_digest,proposal_row.payload_renderer_digest,
    proposal_row.governed_tool_binding_digest,proposal_row.provider_connection_contract_digest,
    proposal_row.provider_capability_digest,proposal_row.provider_operation_id,
    proposal_row.provider_operation_descriptor_digest,decision_row.policy_digest,
    p_command->>'execution_binding_hash',p_command->>'request_hash',
    p_command->>'okg_claim_handoff_receipt_digest',p_command->>'handoff_protected_b64',
    p_command->>'handoff_payload_b64',p_command->>'handoff_signature_b64',
    payload_row.canonical_payload,provider_idempotency_digest_value,
    p_command->>'authentication_audit_id',p_command->>'nomos_decision_id',
    claimed_at_value,proposal_row.execute_before,
    p_command || jsonb_build_object('idempotency_key_hash',p_command->>'idempotency_key_hash')
  ) RETURNING * INTO execution_row;
  UPDATE okg.external_action_proposals SET execution_state='claimed',lifecycle_version=lifecycle_version+1,updated_at=claimed_at_value WHERE deployment_name=bound_deployment AND proposal_id=proposal_row.proposal_id RETURNING * INTO proposal_row;
  event_projection := jsonb_build_object(
    'proposal_id', proposal_row.proposal_id,
    'execution_epoch', proposal_row.execution_epoch,
    'effect_instance_key', proposal_row.effect_instance_key,
    'sequence', event_sequence,
    'lifecycle_version', proposal_row.lifecycle_version,
    'event_type', 'execution_claimed',
    'actor_kind', 'authenticated_principal',
    'actor_principal', p_command ->> 'worker_principal',
    'actor_separation_subject', p_command ->> 'worker_separation_subject',
    'authentication_audit_ref', p_command ->> 'authentication_audit_id',
    'nomos_decision_ref', p_command ->> 'nomos_decision_id',
    'deadline_kind', NULL::TEXT,
    'policy_digest', decision_row.policy_digest,
    'before_review_state', 'approved',
    'after_review_state', 'approved',
    'before_execution_state', 'ready',
    'after_execution_state', 'claimed',
    'payload_version', proposal_row.payload_version,
    'payload_hash', proposal_row.current_payload_hash,
    'decision_id', decision_row.decision_id,
    'decision_binding_hash', decision_row.decision_binding_hash,
    'execution_id', execution_row.execution_id,
    'execution_binding_hash', p_command ->> 'execution_binding_hash',
    'transition_reason_code', NULL::TEXT,
    'outcome', NULL::TEXT,
    'outcome_reason_code', NULL::TEXT,
    'outcome_request_hash', NULL::TEXT,
    'restore_recovery_digest', NULL::TEXT,
    'effective_at', NULL::TEXT,
    'recorded_at', to_char(claimed_at_value AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
    'previous_event_hash', previous_event_hash_value
  );
  event_hash_value := okg.external_action_hash('okg-external-action-event-v1', event_projection);
  claim_event_hash_value := event_hash_value;
  IF p_command ->> 'event_hash' IS DISTINCT FROM event_hash_value
     OR p_command ->> 'claim_event_hash' IS DISTINCT FROM event_hash_value
     OR p_command ->> 'event_chain_head_hash' IS DISTINCT FROM event_hash_value THEN
    RAISE EXCEPTION 'caller-supplied event hash disagrees with the closed event projection'
      USING ERRCODE = '22023';
  END IF;
  INSERT INTO okg.external_action_events (
    deployment_name,proposal_id,execution_epoch,effect_instance_key,sequence,lifecycle_version,
    event_type,actor_kind,actor_principal,actor_separation_subject,authentication_audit_ref,
    nomos_decision_ref,policy_digest,before_review_state,after_review_state,
    before_execution_state,after_execution_state,payload_version,payload_hash,decision_id,
    decision_binding_hash,execution_id,execution_binding_hash,effective_at,recorded_at,
    previous_event_hash,event_hash,store_record
  ) VALUES (
    bound_deployment,proposal_row.proposal_id,proposal_row.execution_epoch,proposal_row.effect_instance_key,
    event_sequence,proposal_row.lifecycle_version,'execution_claimed','authenticated_principal',
    p_command->>'worker_principal',p_command->>'worker_separation_subject',
    p_command->>'authentication_audit_id',p_command->>'nomos_decision_id',decision_row.policy_digest,
    'approved','approved','ready','claimed',proposal_row.payload_version,proposal_row.current_payload_hash,
    decision_row.decision_id,decision_row.decision_binding_hash,execution_row.execution_id,
    p_command->>'execution_binding_hash',NULL,claimed_at_value,
    previous_event_hash_value,event_hash_value,
    jsonb_build_object('event_projection', event_projection)
  );
  RETURN jsonb_build_object(
    'replay',FALSE,'deployment',bound_deployment,
    'proposal_id',proposal_row.proposal_id,
    'execution_id',execution_row.execution_id,
    'decision_id',execution_row.decision_id,
    'execution_state','claimed',
    'lifecycle_version',proposal_row.lifecycle_version,
    'execution_binding_hash',execution_row.execution_binding_hash,
    'provider_idempotency_key',provider_idempotency_key_value,
    'provider_idempotency_key_digest',provider_idempotency_digest_value,
    'okg_claim_handoff_receipt_digest',execution_row.okg_claim_handoff_receipt_digest,
    'handoff_protected_b64',execution_row.handoff_protected_b64,
    'handoff_payload_b64',execution_row.handoff_payload_b64,
    'handoff_signature_b64',execution_row.handoff_signature_b64,
    'canonical_payload',jsonb_build_object(
      '__okg_bytes_b64',replace(
        encode(execution_row.canonical_payload,'base64'),E'\n',''
      )
    ),
    'event_hash',event_hash_value
  );
END
$external_action_claim$;
CREATE OR REPLACE FUNCTION okg.external_action_report_outcome(p_command JSONB)
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, okg AS $external_action_report_outcome$
DECLARE
  bound_deployment TEXT := okg.external_action_session_deployment();
  execution_row okg.external_action_executions%ROWTYPE;
  proposal_row okg.external_action_proposals%ROWTYPE;
  decision_row okg.external_action_decisions%ROWTYPE;
  outcome_row okg.external_action_outcomes%ROWTYPE;
  existing_outcome okg.external_action_outcomes%ROWTYPE;
  claim_auth_provider_id TEXT; claim_auth_binding_id TEXT; claim_auth_subject TEXT;
  claim_provider_status TEXT; claim_provider_updated_at TIMESTAMPTZ;
  claim_binding_status TEXT; claim_binding_updated_at TIMESTAMPTZ;
  claim_binding_subject TEXT;
  reconciler_value BOOLEAN := FALSE;
  observed_at TIMESTAMPTZ := clock_timestamp();
  event_sequence BIGINT;
  previous_event_hash_value TEXT;
  event_hash_value TEXT;
  event_projection JSONB;
  contract_rule JSONB;
  canonical_outcome_request_hash TEXT;
  audit_subject TEXT;
BEGIN
  PERFORM okg.external_action_verifier_capability_status();
  PERFORM okg.external_action_require_command(p_command,ARRAY[
    'deployment','execution_id','effect_instance_key','worker_or_reconciler_separation_subject','outcome','outcome_reason_code','provider_receipt_digest','recovery_journal_digest','owner_unavailability_attestation_digest','outcome_request_hash','idempotency_key_hash','authentication_audit_id','nomos_decision_id','policy_digest','reconciler','event_hash','previous_event_hash','if_match','expected_lifecycle_version','expected_payload_version','expected_payload_hash'
  ],ARRAY[
    'deployment','execution_id','effect_instance_key','worker_or_reconciler_separation_subject','outcome','outcome_reason_code','recovery_journal_digest','outcome_request_hash','idempotency_key_hash','authentication_audit_id','nomos_decision_id','policy_digest','if_match','expected_lifecycle_version','expected_payload_version','expected_payload_hash'
  ]);
  IF p_command->>'deployment'<>bound_deployment THEN RAISE EXCEPTION 'external action deployment is not bound to session_user' USING ERRCODE='28000'; END IF;
  reconciler_value:=COALESCE((p_command->>'reconciler')::boolean,FALSE);
  PERFORM pg_advisory_xact_lock(hashtextextended('okg.external_action.deployment:'||bound_deployment,0));
  SELECT * INTO execution_row FROM okg.external_action_executions WHERE deployment_name=bound_deployment AND execution_id=p_command->>'execution_id' FOR UPDATE;
  IF execution_row.execution_id IS NULL THEN RAISE EXCEPTION 'external action execution was not found' USING ERRCODE='P0002'; END IF;
  IF p_command->>'effect_instance_key'<>execution_row.effect_instance_key THEN RAISE EXCEPTION 'external action effect binding is stale' USING ERRCODE='40001'; END IF;
  -- Revalidate live authority before resolving an exact replay.  A replay
  -- cannot bypass current Nomos authority or claim-owner revocation.
  PERFORM okg.external_action_nomos_authority_snapshot(bound_deployment,p_command->>'authentication_audit_id',p_command->>'nomos_decision_id',CASE WHEN reconciler_value THEN 'external_action.reconcile' ELSE 'external_action.report' END,p_command->>'outcome_request_hash');
  SELECT a.separation_subject INTO audit_subject
    FROM okg.external_action_auth_audits a
   WHERE a.deployment_name=bound_deployment
     AND a.authentication_audit_id=p_command->>'authentication_audit_id';
  IF audit_subject IS NULL OR p_command->>'worker_or_reconciler_separation_subject' IS DISTINCT FROM audit_subject THEN
    RAISE EXCEPTION 'outcome actor is not the retained authenticated subject' USING ERRCODE='42501';
  END IF;
  IF NOT reconciler_value AND audit_subject<>execution_row.worker_separation_subject THEN RAISE EXCEPTION 'only the claiming worker may report the outcome' USING ERRCODE='42501'; END IF;
  IF NOT reconciler_value AND p_command->>'owner_unavailability_attestation_digest' IS NOT NULL THEN RAISE EXCEPTION 'claim-owner reports cannot include an unavailability attestation' USING ERRCODE='22023'; END IF;
  IF reconciler_value THEN
    SELECT a.nomos_provider_id,a.nomos_subject_binding_id,a.separation_subject,p.status,p.updated_at,b.status,b.updated_at,b.subject
      INTO claim_auth_provider_id,claim_auth_binding_id,claim_auth_subject,claim_provider_status,claim_provider_updated_at,claim_binding_status,claim_binding_updated_at,claim_binding_subject
      FROM okg.external_action_auth_audits a
      JOIN okg.nomos_identity_providers p ON p.deployment_name=a.deployment_name AND p.provider_id=a.nomos_provider_id
      JOIN okg.nomos_subject_bindings b ON b.deployment_name=a.deployment_name AND b.binding_id=a.nomos_subject_binding_id AND b.provider_id=p.provider_id AND b.issuer=p.issuer AND b.subject=a.separation_subject
     WHERE a.deployment_name=bound_deployment AND a.authentication_audit_id=execution_row.authentication_audit_id AND a.separation_subject=execution_row.worker_separation_subject;
    IF claim_auth_provider_id IS NULL OR claim_auth_binding_id IS NULL OR claim_auth_subject IS DISTINCT FROM execution_row.worker_separation_subject OR claim_binding_subject IS DISTINCT FROM execution_row.worker_separation_subject OR NOT ((claim_provider_status IN ('disabled','revoked') AND claim_provider_updated_at>execution_row.claimed_at) OR (claim_binding_status IN ('disabled','revoked') AND claim_binding_updated_at>execution_row.claimed_at)) THEN RAISE EXCEPTION 'reconciliation requires the exact claim identity to be disabled or revoked after claim' USING ERRCODE='42501'; END IF;
  IF EXISTS (SELECT 1 FROM okg.nomos_identity_providers p JOIN okg.nomos_subject_bindings b ON b.deployment_name=p.deployment_name AND b.provider_id=p.provider_id AND b.issuer=p.issuer WHERE p.deployment_name=bound_deployment AND b.subject=execution_row.worker_separation_subject AND NOT (p.provider_id=claim_auth_provider_id AND b.binding_id=claim_auth_binding_id) AND p.status='active' AND b.status='active' AND (p.effective_from IS NULL OR p.effective_from<=observed_at) AND (p.effective_until IS NULL OR observed_at<p.effective_until) AND (b.effective_from IS NULL OR b.effective_from<=observed_at) AND (b.effective_until IS NULL OR observed_at<b.effective_until)) THEN RAISE EXCEPTION 'reconciliation is blocked by an active replacement identity binding' USING ERRCODE='42501'; END IF;
    IF NOT (p_command ? 'owner_unavailability_attestation_digest') OR NOT EXISTS (SELECT 1 FROM okg.external_action_owner_unavailability_attestations a WHERE a.deployment_name=bound_deployment AND a.execution_id=execution_row.execution_id AND a.claim_owner_separation_subject=execution_row.worker_separation_subject AND a.envelope_digest=p_command->>'owner_unavailability_attestation_digest' AND a.reconcile_not_before<=observed_at AND observed_at<a.expires_at) THEN RAISE EXCEPTION 'reconciliation requires a current owner-unavailability attestation' USING ERRCODE='40001'; END IF;
  END IF;
  SELECT * INTO proposal_row FROM okg.external_action_proposals WHERE deployment_name=bound_deployment AND proposal_id=execution_row.proposal_id FOR SHARE;
  IF proposal_row.proposal_id IS NULL THEN RAISE EXCEPTION 'external action proposal was not found' USING ERRCODE='P0002'; END IF;
  SELECT * INTO decision_row FROM okg.external_action_decisions
   WHERE deployment_name=bound_deployment AND decision_id=execution_row.decision_id
   FOR SHARE;
  IF decision_row.decision_id IS NULL THEN
    RAISE EXCEPTION 'external action decision was not found' USING ERRCODE='P0002';
  END IF;
  IF p_command->>'policy_digest' IS DISTINCT FROM decision_row.policy_digest
     OR execution_row.policy_digest IS DISTINCT FROM decision_row.policy_digest THEN
    RAISE EXCEPTION 'external action outcome policy binding is stale' USING ERRCODE='40001';
  END IF;
  IF NOT (proposal_row.action_contract_record ? 'outcome_reason_rules') THEN
    RAISE EXCEPTION 'external action contract authority is unavailable'
      USING ERRCODE='40001';
  END IF;
    contract_rule:=proposal_row.action_contract_record->'outcome_reason_rules'->(p_command->>'outcome_reason_code');
    IF jsonb_typeof(contract_rule) IS DISTINCT FROM 'object'
       OR NOT (contract_rule->'allowed_outcomes' ? (p_command->>'outcome'))
       OR contract_rule->>'provider_receipt' NOT IN ('required','forbidden')
       OR ((contract_rule->>'provider_receipt')='required') IS DISTINCT FROM (p_command->>'provider_receipt_digest' IS NOT NULL) THEN
      RAISE EXCEPTION 'outcome is not allowed by the retained action contract' USING ERRCODE='22023';
    END IF;
  IF p_command->>'provider_receipt_digest' IS NOT NULL
     AND p_command->>'provider_receipt_digest' !~ '^sha256:[0-9a-f]{64}$' THEN
    RAISE EXCEPTION 'provider receipt digest is not canonical' USING ERRCODE='22023';
  END IF;
    canonical_outcome_request_hash:=okg.external_action_hash(
      'okg-external-action-outcome-request-v1',jsonb_build_object(
        'execution_id',execution_row.execution_id,
        'effect_instance_key',execution_row.effect_instance_key,
        'worker_or_reconciler_separation_subject',audit_subject,
        'outcome',p_command->>'outcome',
        'outcome_reason_code',p_command->>'outcome_reason_code',
        'provider_receipt_digest',p_command->>'provider_receipt_digest',
        'recovery_journal_digest',p_command->>'recovery_journal_digest',
        'owner_unavailability_attestation_digest',p_command->>'owner_unavailability_attestation_digest',
        'if_match',p_command->>'if_match'));
    IF p_command->>'outcome_request_hash' IS DISTINCT FROM canonical_outcome_request_hash THEN
      RAISE EXCEPTION 'outcome request hash is not canonical' USING ERRCODE='40001';
    END IF;
  SELECT * INTO existing_outcome FROM okg.external_action_outcomes WHERE deployment_name=bound_deployment AND execution_id=execution_row.execution_id FOR UPDATE;
  IF existing_outcome.outcome_id IS NOT NULL THEN
    IF existing_outcome.request_hash=p_command->>'outcome_request_hash' AND existing_outcome.store_record->>'idempotency_key_hash'=p_command->>'idempotency_key_hash' THEN
      RETURN jsonb_build_object('replay',TRUE,'deployment',bound_deployment,'execution_id',existing_outcome.execution_id,'effect_instance_key',existing_outcome.effect_instance_key,'outcome',existing_outcome.outcome,'outcome_request_hash',existing_outcome.request_hash);
    END IF;
    RAISE EXCEPTION 'external action outcome has already been recorded' USING ERRCODE='40001';
  END IF;
  IF p_command->>'if_match' IS DISTINCT FROM format(
       '"lifecycle:%s;payload-version:%s;payload:%s"',
       p_command->>'expected_lifecycle_version',
       p_command->>'expected_payload_version',
       p_command->>'expected_payload_hash'
     )
     OR (p_command->>'expected_lifecycle_version')::bigint IS DISTINCT FROM proposal_row.lifecycle_version
     OR (p_command->>'expected_payload_version')::bigint IS DISTINCT FROM proposal_row.payload_version
     OR p_command->>'expected_payload_hash' IS DISTINCT FROM proposal_row.current_payload_hash THEN
    RAISE EXCEPTION 'external action outcome precondition is stale'
      USING ERRCODE='40001';
  END IF;
  IF reconciler_value AND (p_command->>'worker_or_reconciler_separation_subject'=execution_row.worker_separation_subject OR p_command->>'outcome'<>'indeterminate' OR NOT (p_command ? 'owner_unavailability_attestation_digest')) THEN RAISE EXCEPTION 'reconciliation requires an independent indeterminate outcome' USING ERRCODE='42501'; END IF;
  IF p_command->>'outcome' NOT IN ('succeeded','failed','indeterminate') THEN RAISE EXCEPTION 'external action outcome is not allowed' USING ERRCODE='22023'; END IF;
  IF p_command->>'outcome'='succeeded' AND NOT (p_command ? 'provider_receipt_digest') THEN RAISE EXCEPTION 'successful outcome requires a provider receipt' USING ERRCODE='22023'; END IF;
  IF reconciler_value AND p_command ? 'provider_receipt_digest' THEN RAISE EXCEPTION 'reconciliation cannot provide a provider receipt' USING ERRCODE='22023'; END IF;
  IF p_command->>'owner_unavailability_attestation_digest' IS NOT NULL AND NOT reconciler_value AND NOT EXISTS (SELECT 1 FROM okg.external_action_owner_unavailability_attestations a WHERE a.deployment_name=bound_deployment AND a.execution_id=execution_row.execution_id AND a.envelope_digest=p_command->>'owner_unavailability_attestation_digest') THEN RAISE EXCEPTION 'owner-unavailability attestation is not bound' USING ERRCODE='40001'; END IF;
  SELECT * INTO proposal_row FROM okg.external_action_proposals WHERE deployment_name=bound_deployment AND proposal_id=execution_row.proposal_id FOR UPDATE;
  INSERT INTO okg.external_action_outcomes (deployment_name,execution_id,effect_instance_key,worker_or_reconciler_separation_subject,outcome,outcome_reason_code,provider_receipt_digest,recovery_journal_digest,owner_unavailability_attestation_digest,request_hash,authentication_audit_id,nomos_decision_id,recorded_at,store_record) VALUES (bound_deployment,execution_row.execution_id,execution_row.effect_instance_key,p_command->>'worker_or_reconciler_separation_subject',p_command->>'outcome',p_command->>'outcome_reason_code',p_command->>'provider_receipt_digest',p_command->>'recovery_journal_digest',p_command->>'owner_unavailability_attestation_digest',p_command->>'outcome_request_hash',p_command->>'authentication_audit_id',p_command->>'nomos_decision_id',observed_at,p_command||jsonb_build_object('idempotency_key_hash',p_command->>'idempotency_key_hash')) RETURNING * INTO outcome_row;
  UPDATE okg.external_action_proposals SET execution_state=p_command->>'outcome',lifecycle_version=lifecycle_version+1,updated_at=observed_at WHERE deployment_name=bound_deployment AND proposal_id=proposal_row.proposal_id RETURNING * INTO proposal_row;
  SELECT COALESCE(max(sequence),0)+1,(array_agg(event_hash ORDER BY sequence))[array_length(array_agg(event_hash ORDER BY sequence),1)] INTO event_sequence,previous_event_hash_value FROM okg.external_action_events WHERE deployment_name=bound_deployment AND effect_instance_key=execution_row.effect_instance_key;
  IF p_command->>'previous_event_hash' IS NOT NULL AND p_command->>'previous_event_hash' IS DISTINCT FROM previous_event_hash_value THEN RAISE EXCEPTION 'external action event chain predecessor is stale' USING ERRCODE='40001'; END IF;
  event_projection := jsonb_build_object(
    'proposal_id', proposal_row.proposal_id,
    'execution_epoch', proposal_row.execution_epoch,
    'effect_instance_key', execution_row.effect_instance_key,
    'sequence', event_sequence,
    'lifecycle_version', proposal_row.lifecycle_version,
    'event_type', 'outcome_reported',
    'actor_kind', 'authenticated_principal',
    'actor_principal', NULL::TEXT,
    'actor_separation_subject', p_command->>'worker_or_reconciler_separation_subject',
    'authentication_audit_ref', p_command->>'authentication_audit_id',
    'nomos_decision_ref', p_command->>'nomos_decision_id',
    'deadline_kind', NULL::TEXT,
    'policy_digest', decision_row.policy_digest,
    'before_review_state', proposal_row.review_state,
    'after_review_state', proposal_row.review_state,
    'before_execution_state', 'claimed',
    'after_execution_state', p_command->>'outcome',
    'payload_version', proposal_row.payload_version,
    'payload_hash', proposal_row.current_payload_hash,
    'decision_id', execution_row.decision_id,
    'decision_binding_hash', decision_row.decision_binding_hash,
    'execution_id', execution_row.execution_id,
    'execution_binding_hash', execution_row.execution_binding_hash,
    'transition_reason_code', NULL::TEXT,
    'outcome', p_command->>'outcome',
    'outcome_reason_code', p_command->>'outcome_reason_code',
    'outcome_request_hash', p_command->>'outcome_request_hash',
    'restore_recovery_digest', NULL::TEXT,
    'effective_at', NULL::TEXT,
    'recorded_at', to_char(observed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
    'previous_event_hash', previous_event_hash_value
  );
  event_hash_value:=okg.external_action_hash('okg-external-action-event-v1',event_projection);
  IF p_command->>'event_hash' IS NOT NULL
     AND p_command->>'event_hash' IS DISTINCT FROM event_hash_value THEN
    RAISE EXCEPTION 'caller-supplied event hash disagrees with the closed event projection' USING ERRCODE='22023';
  END IF;
  INSERT INTO okg.external_action_events (deployment_name,proposal_id,execution_epoch,effect_instance_key,sequence,lifecycle_version,event_type,actor_kind,actor_principal,actor_separation_subject,authentication_audit_ref,nomos_decision_ref,deadline_kind,policy_digest,before_review_state,after_review_state,before_execution_state,after_execution_state,payload_version,payload_hash,decision_id,decision_binding_hash,execution_id,execution_binding_hash,transition_reason_code,outcome,outcome_reason_code,outcome_request_hash,restore_recovery_digest,effective_at,recorded_at,previous_event_hash,event_hash,store_record) VALUES (bound_deployment,proposal_row.proposal_id,proposal_row.execution_epoch,execution_row.effect_instance_key,event_sequence,proposal_row.lifecycle_version,'outcome_reported','authenticated_principal',NULL,p_command->>'worker_or_reconciler_separation_subject',p_command->>'authentication_audit_id',p_command->>'nomos_decision_id',NULL,decision_row.policy_digest,proposal_row.review_state,proposal_row.review_state,'claimed',p_command->>'outcome',proposal_row.payload_version,proposal_row.current_payload_hash,execution_row.decision_id,decision_row.decision_binding_hash,execution_row.execution_id,execution_row.execution_binding_hash,NULL,p_command->>'outcome',p_command->>'outcome_reason_code',p_command->>'outcome_request_hash',NULL,NULL,observed_at,previous_event_hash_value,event_hash_value,jsonb_build_object('event_projection',event_projection));
  RETURN jsonb_build_object('replay',FALSE,'deployment',bound_deployment,'execution_id',execution_row.execution_id,'effect_instance_key',execution_row.effect_instance_key,'outcome',outcome_row.outcome,'outcome_request_hash',outcome_row.request_hash,'event_hash',event_hash_value);
END
$external_action_report_outcome$;

CREATE OR REPLACE FUNCTION okg.external_action_owner_unavailability(p_command JSONB)
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, okg AS $external_action_owner_unavailability$
DECLARE
  bound_deployment TEXT := okg.external_action_session_deployment();
  execution_row okg.external_action_executions%ROWTYPE;
  proposal_row okg.external_action_proposals%ROWTYPE;
  attestation_row okg.external_action_owner_unavailability_attestations%ROWTYPE;
  existing_row okg.external_action_owner_unavailability_attestations%ROWTYPE;
  observed_at TIMESTAMPTZ := clock_timestamp();
  audit_subject TEXT;
  expected_reconcile_not_before TIMESTAMPTZ;
  expected_request_hash TEXT;
  expected_envelope_digest TEXT;
  expected_response_body BYTEA;
BEGIN
  PERFORM okg.external_action_verifier_capability_status();
  PERFORM okg.external_action_require_command(p_command,ARRAY['deployment','execution_id','claim_owner_separation_subject','reconcile_not_before','issued_at','expires_at','nonce','signer_separation_subject','authenticated_submitter_separation_subject','idempotency_key_hash','request_hash','response_status','response_body_jcs','protected_b64','payload_b64','signature_b64','signer_key_id_digest','envelope_digest','authentication_audit_id','nomos_decision_id'],ARRAY['deployment','execution_id','claim_owner_separation_subject','reconcile_not_before','issued_at','expires_at','nonce','signer_separation_subject','authenticated_submitter_separation_subject','idempotency_key_hash','request_hash','response_status','response_body_jcs','protected_b64','payload_b64','signature_b64','signer_key_id_digest','envelope_digest','authentication_audit_id','nomos_decision_id']);
  IF p_command->>'deployment'<>bound_deployment THEN RAISE EXCEPTION 'external action deployment is not bound to session_user' USING ERRCODE='28000'; END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended('okg.external_action.deployment:'||bound_deployment,0));
  SELECT * INTO execution_row FROM okg.external_action_executions WHERE deployment_name=bound_deployment AND execution_id=p_command->>'execution_id' FOR SHARE;
  IF execution_row.execution_id IS NULL THEN RAISE EXCEPTION 'external action execution was not found' USING ERRCODE='P0002'; END IF;
  SELECT * INTO proposal_row FROM okg.external_action_proposals
   WHERE deployment_name=bound_deployment AND proposal_id=execution_row.proposal_id
   FOR SHARE;
  IF proposal_row.proposal_id IS NULL
     OR NOT (proposal_row.action_contract_record ? 'review_and_execution_limits') THEN
    RAISE EXCEPTION 'external action contract authority is unavailable' USING ERRCODE='40001';
  END IF;
  PERFORM okg.external_action_nomos_authority_snapshot(bound_deployment,p_command->>'authentication_audit_id',p_command->>'nomos_decision_id','external_action.attest_owner_unavailable',p_command->>'request_hash');
  SELECT separation_subject INTO audit_subject
    FROM okg.external_action_auth_audits
   WHERE deployment_name=bound_deployment
     AND authentication_audit_id=p_command->>'authentication_audit_id';
  expected_reconcile_not_before := execution_row.claimed_at + make_interval(
    secs => (proposal_row.action_contract_record->'review_and_execution_limits'->>'reconcile_not_before_seconds')::double precision
  );
  expected_request_hash := okg.external_action_hash(
    'okg-external-action-transition-request-v1',
    jsonb_build_object(
      'protected_b64',p_command->>'protected_b64',
      'payload_b64',p_command->>'payload_b64',
      'signature_b64',p_command->>'signature_b64'
    )
  );
  expected_envelope_digest := okg.external_action_hash(
    'okg-external-action-owner-unavailability-attestation-envelope-v1',
    jsonb_build_object(
      'protected_b64',p_command->>'protected_b64',
      'payload_b64',p_command->>'payload_b64',
      'signature_b64',p_command->>'signature_b64'
    )
  );
  expected_response_body := convert_to(
    okg.jsonb_canonical_text(jsonb_build_object(
      'protected_b64',p_command->>'protected_b64',
      'payload_b64',p_command->>'payload_b64',
      'signature_b64',p_command->>'signature_b64',
      'envelope_digest',expected_envelope_digest
    )),
    'UTF8'
  );
  IF audit_subject IS NULL
     OR p_command->>'authenticated_submitter_separation_subject' IS DISTINCT FROM audit_subject
     OR p_command->>'signer_separation_subject' IS DISTINCT FROM audit_subject
     OR p_command->>'claim_owner_separation_subject' IS DISTINCT FROM execution_row.worker_separation_subject
     OR p_command->>'signer_separation_subject'=execution_row.worker_separation_subject THEN
    RAISE EXCEPTION 'owner-unavailability attestation subject binding is invalid' USING ERRCODE='42501';
  END IF;
  IF p_command->>'request_hash' IS DISTINCT FROM expected_request_hash
     OR p_command->>'envelope_digest' IS DISTINCT FROM expected_envelope_digest
     OR (p_command->>'response_status')::integer<>201
     OR okg.external_action_json_bytes(p_command->'response_body_jcs') IS DISTINCT FROM expected_response_body THEN
    RAISE EXCEPTION 'owner-unavailability attestation evidence is not canonical' USING ERRCODE='22023';
  END IF;
  IF p_command->>'reconcile_not_before' !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z$'
     OR p_command->>'issued_at' !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z$'
     OR p_command->>'expires_at' !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z$'
     OR (p_command->>'reconcile_not_before')::timestamptz IS DISTINCT FROM expected_reconcile_not_before
     OR (p_command->>'issued_at')::timestamptz<expected_reconcile_not_before
     OR (p_command->>'issued_at')::timestamptz>observed_at
     OR (p_command->>'expires_at')::timestamptz<=(p_command->>'issued_at')::timestamptz
     OR (p_command->>'expires_at')::timestamptz-(p_command->>'issued_at')::timestamptz>interval '1 day'
     OR p_command->>'nonce' !~ '^[A-Za-z0-9._:+/\-]{16,128}$' THEN
    RAISE EXCEPTION 'owner-unavailability attestation validity interval is invalid' USING ERRCODE='22023';
  END IF;
  SELECT * INTO existing_row FROM okg.external_action_owner_unavailability_attestations WHERE deployment_name=bound_deployment AND envelope_digest=p_command->>'envelope_digest' FOR UPDATE;
  IF existing_row.attestation_id IS NOT NULL THEN
    IF existing_row.request_hash IS DISTINCT FROM p_command->>'request_hash'
       OR existing_row.idempotency_key_hash IS DISTINCT FROM p_command->>'idempotency_key_hash'
       OR existing_row.authenticated_submitter_separation_subject IS DISTINCT FROM audit_subject
       OR existing_row.store_record IS DISTINCT FROM p_command THEN
      RAISE EXCEPTION 'owner-unavailability attestation replay conflicts' USING ERRCODE='40001';
    END IF;
    RETURN jsonb_build_object('replay',TRUE,'deployment',bound_deployment,'execution_id',existing_row.execution_id,'envelope_digest',existing_row.envelope_digest);
  END IF;
  IF observed_at>=(p_command->>'expires_at')::timestamptz THEN
    RAISE EXCEPTION 'owner-unavailability attestation has expired' USING ERRCODE='22008';
  END IF;
  INSERT INTO okg.external_action_owner_unavailability_attestations (deployment_name,execution_id,claim_owner_separation_subject,reconcile_not_before,issued_at,expires_at,nonce,signer_separation_subject,authenticated_submitter_separation_subject,idempotency_key_hash,request_hash,response_status,response_body_jcs,protected_b64,payload_b64,signature_b64,signer_key_id_digest,envelope_digest,verified_at,authentication_audit_id,nomos_decision_id,store_record) VALUES (bound_deployment,p_command->>'execution_id',p_command->>'claim_owner_separation_subject',(p_command->>'reconcile_not_before')::timestamptz,(p_command->>'issued_at')::timestamptz,(p_command->>'expires_at')::timestamptz,p_command->>'nonce',p_command->>'signer_separation_subject',p_command->>'authenticated_submitter_separation_subject',p_command->>'idempotency_key_hash',p_command->>'request_hash',(p_command->>'response_status')::integer,okg.external_action_json_bytes(p_command->'response_body_jcs'),p_command->>'protected_b64',p_command->>'payload_b64',p_command->>'signature_b64',p_command->>'signer_key_id_digest',p_command->>'envelope_digest',observed_at,p_command->>'authentication_audit_id',p_command->>'nomos_decision_id',p_command) RETURNING * INTO attestation_row;
  RETURN jsonb_build_object('replay',FALSE,'deployment',bound_deployment,'execution_id',attestation_row.execution_id,'envelope_digest',attestation_row.envelope_digest,'request_hash',attestation_row.request_hash);
END
$external_action_owner_unavailability$;
CREATE OR REPLACE FUNCTION okg.external_action_restore_create(p_command JSONB)
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, okg AS $external_action_restore_create$
DECLARE bound_deployment TEXT:=okg.external_action_session_deployment(); fence_row okg.external_action_restore_fences%ROWTYPE; attestation_row okg.external_action_restore_attestations%ROWTYPE; session_row okg.external_action_restore_sessions%ROWTYPE; old_session okg.external_action_restore_sessions%ROWTYPE; completion_row okg.external_action_restore_completion_receipts%ROWTYPE; observed_at TIMESTAMPTZ:=clock_timestamp();
BEGIN
  PERFORM okg.external_action_require_command(p_command,ARRAY['deployment','restore_id','old_epoch','new_epoch','old_epoch_lease_digest','new_epoch_lease_digest','ledger_fence_envelope_digest','fenced_ledger_root_digest','fenced_ledger_entry_count','restored_snapshot_digest','pre_restore_event_root_digest','pre_restore_event_head_count','effect_inventory_digest','effect_inventory_count','worker_checkpoint_set_digest','worker_checkpoint_count','issued_at','expires_at','nonce','signer_separation_subject','protected_b64','payload_b64','signature_b64','signer_key_id_digest','envelope_digest','authentication_audit_id','nomos_decision_id','operator_separation_subject','request_hash','idempotency_key_hash','status'],ARRAY['deployment','restore_id','old_epoch','new_epoch','old_epoch_lease_digest','new_epoch_lease_digest','ledger_fence_envelope_digest','fenced_ledger_root_digest','fenced_ledger_entry_count','restored_snapshot_digest','pre_restore_event_root_digest','pre_restore_event_head_count','effect_inventory_digest','effect_inventory_count','worker_checkpoint_set_digest','worker_checkpoint_count','issued_at','expires_at','nonce','signer_separation_subject','protected_b64','payload_b64','signature_b64','signer_key_id_digest','envelope_digest','authentication_audit_id','nomos_decision_id','operator_separation_subject','request_hash','idempotency_key_hash']);
  IF p_command->>'deployment'<>bound_deployment THEN RAISE EXCEPTION 'external action deployment is not bound to session_user' USING ERRCODE='28000'; END IF;
  IF (p_command->>'new_epoch')::bigint<=(p_command->>'old_epoch')::bigint THEN RAISE EXCEPTION 'restore epoch must advance' USING ERRCODE='40001'; END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended('okg.external_action.deployment:'||bound_deployment,0));
  PERFORM okg.external_action_nomos_authority_snapshot(bound_deployment,p_command->>'authentication_audit_id',p_command->>'nomos_decision_id','external_action.restore_reconcile',p_command->>'request_hash');
  SELECT * INTO fence_row FROM okg.external_action_restore_fences WHERE deployment_name=bound_deployment FOR UPDATE;
  IF fence_row.deployment_name IS NOT NULL AND fence_row.highest_accepted_epoch>=(p_command->>'new_epoch')::bigint THEN RAISE EXCEPTION 'restore epoch is not newer than accepted fence' USING ERRCODE='40001'; END IF;
  SELECT * INTO old_session FROM okg.external_action_restore_sessions WHERE deployment_name=bound_deployment AND restore_id=p_command->>'restore_id' FOR UPDATE;
  IF old_session.restore_session_row_id IS NOT NULL THEN
    IF old_session.request_hash<>p_command->>'request_hash' THEN RAISE EXCEPTION 'restore id was reused with a different request' USING ERRCODE='40001'; END IF;
    SELECT * INTO completion_row FROM okg.external_action_restore_completion_receipts WHERE deployment_name=bound_deployment AND restore_id=old_session.restore_id FOR SHARE;
    RETURN jsonb_build_object('replay',TRUE,'deployment',bound_deployment,'restore_id',old_session.restore_id,'status',CASE WHEN completion_row.completion_receipt_row_id IS NULL THEN old_session.status ELSE 'complete' END);
  END IF;
  INSERT INTO okg.external_action_restore_attestations (deployment_name,restore_id,old_epoch,new_epoch,old_epoch_lease_digest,new_epoch_lease_digest,ledger_fence_envelope_digest,fenced_ledger_root_digest,fenced_ledger_entry_count,restored_snapshot_digest,pre_restore_event_root_digest,pre_restore_event_head_count,effect_inventory_digest,effect_inventory_count,worker_checkpoint_set_digest,worker_checkpoint_count,issued_at,expires_at,nonce,signer_separation_subject,protected_b64,payload_b64,signature_b64,signer_key_id_digest,envelope_digest,verified_at,authentication_audit_id,nomos_decision_id,store_record) VALUES (bound_deployment,p_command->>'restore_id',(p_command->>'old_epoch')::bigint,(p_command->>'new_epoch')::bigint,p_command->>'old_epoch_lease_digest',p_command->>'new_epoch_lease_digest',p_command->>'ledger_fence_envelope_digest',p_command->>'fenced_ledger_root_digest',(p_command->>'fenced_ledger_entry_count')::bigint,p_command->>'restored_snapshot_digest',p_command->>'pre_restore_event_root_digest',(p_command->>'pre_restore_event_head_count')::bigint,p_command->>'effect_inventory_digest',(p_command->>'effect_inventory_count')::bigint,p_command->>'worker_checkpoint_set_digest',(p_command->>'worker_checkpoint_count')::bigint,(p_command->>'issued_at')::timestamptz,(p_command->>'expires_at')::timestamptz,p_command->>'nonce',p_command->>'signer_separation_subject',p_command->>'protected_b64',p_command->>'payload_b64',p_command->>'signature_b64',p_command->>'signer_key_id_digest',p_command->>'envelope_digest',observed_at,p_command->>'authentication_audit_id',p_command->>'nomos_decision_id',p_command) RETURNING * INTO attestation_row;
  INSERT INTO okg.external_action_restore_sessions (deployment_name,restore_id,request_hash,idempotency_key_hash,operator_separation_subject,authentication_audit_id,nomos_decision_id,restore_attestation_envelope_digest,status,created_at,updated_at,store_record) VALUES (bound_deployment,p_command->>'restore_id',p_command->>'request_hash',p_command->>'idempotency_key_hash',p_command->>'operator_separation_subject',p_command->>'authentication_audit_id',p_command->>'nomos_decision_id',attestation_row.envelope_digest,COALESCE(p_command->>'status','staging'),observed_at,observed_at,p_command) RETURNING * INTO session_row;
  RETURN jsonb_build_object('replay',FALSE,'deployment',bound_deployment,'restore_id',session_row.restore_id,'status',session_row.status,'restore_attestation_envelope_digest',session_row.restore_attestation_envelope_digest);
END
$external_action_restore_create$;

CREATE OR REPLACE FUNCTION okg.external_action_restore_batch(p_command JSONB)
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, okg AS $external_action_restore_batch$
DECLARE bound_deployment TEXT:=okg.external_action_session_deployment(); session_row okg.external_action_restore_sessions%ROWTYPE; checkpoint_row okg.external_action_restore_checkpoint_staging%ROWTYPE; existing_row okg.external_action_restore_checkpoint_staging%ROWTYPE;
BEGIN
  PERFORM okg.external_action_require_command(p_command,ARRAY['deployment','restore_id','batch_index','request_hash','checkpoint_envelope_digest','proof_digest','checkpoint_payload','authentication_audit_id','nomos_decision_id'],ARRAY['deployment','restore_id','batch_index','request_hash','checkpoint_envelope_digest','proof_digest','checkpoint_payload','authentication_audit_id','nomos_decision_id']);
  IF p_command->>'deployment'<>bound_deployment THEN RAISE EXCEPTION 'external action deployment is not bound to session_user' USING ERRCODE='28000'; END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended('okg.external_action.deployment:'||bound_deployment,0));
  PERFORM okg.external_action_nomos_authority_snapshot(bound_deployment,p_command->>'authentication_audit_id',p_command->>'nomos_decision_id','external_action.restore_reconcile',p_command->>'request_hash');
  SELECT * INTO session_row FROM okg.external_action_restore_sessions WHERE deployment_name=bound_deployment AND restore_id=p_command->>'restore_id' FOR UPDATE;
  IF session_row.restore_session_row_id IS NULL OR session_row.status NOT IN ('staging','quarantined') THEN RAISE EXCEPTION 'restore session is not accepting checkpoint batches' USING ERRCODE='40001'; END IF;
  SELECT * INTO existing_row FROM okg.external_action_restore_checkpoint_staging WHERE deployment_name=bound_deployment AND restore_id=session_row.restore_id AND batch_index=(p_command->>'batch_index')::bigint FOR UPDATE;
  IF existing_row.checkpoint_staging_row_id IS NOT NULL THEN
    IF existing_row.request_hash<>p_command->>'request_hash' THEN RAISE EXCEPTION 'restore batch index was reused with a different request' USING ERRCODE='40001'; END IF;
    RETURN jsonb_build_object('replay',TRUE,'deployment',bound_deployment,'restore_id',session_row.restore_id,'batch_index',existing_row.batch_index,'checkpoint_envelope_digest',existing_row.checkpoint_envelope_digest);
  END IF;
  INSERT INTO okg.external_action_restore_checkpoint_staging (deployment_name,restore_id,batch_index,request_hash,checkpoint_envelope_digest,proof_digest,checkpoint_payload,staged_at,store_record) VALUES (bound_deployment,session_row.restore_id,(p_command->>'batch_index')::bigint,p_command->>'request_hash',p_command->>'checkpoint_envelope_digest',p_command->>'proof_digest',p_command->'checkpoint_payload',clock_timestamp(),p_command) RETURNING * INTO checkpoint_row;
  RETURN jsonb_build_object('replay',FALSE,'deployment',bound_deployment,'restore_id',checkpoint_row.restore_id,'batch_index',checkpoint_row.batch_index,'checkpoint_envelope_digest',checkpoint_row.checkpoint_envelope_digest);
END
$external_action_restore_batch$;

CREATE OR REPLACE FUNCTION okg.external_action_restore_finalize(p_command JSONB)
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, okg AS $external_action_restore_finalize$
DECLARE bound_deployment TEXT:=okg.external_action_session_deployment(); session_row okg.external_action_restore_sessions%ROWTYPE; completion_row okg.external_action_restore_completion_receipts%ROWTYPE; attestation_row okg.external_action_restore_attestations%ROWTYPE; fence_row okg.external_action_restore_fences%ROWTYPE; observed_at TIMESTAMPTZ:=clock_timestamp();
BEGIN
  PERFORM okg.external_action_require_command(p_command,ARRAY['deployment','restore_id','new_epoch','new_epoch_lease_digest','lease_chain_digest','verification_config_digest','verification_login_binding_digest','service_mode','quarantine_state','completion_receipt_envelope_digest','authentication_audit_id','nomos_decision_id'],ARRAY['deployment','restore_id','new_epoch','new_epoch_lease_digest','lease_chain_digest','verification_config_digest','verification_login_binding_digest','service_mode','quarantine_state','completion_receipt_envelope_digest','authentication_audit_id','nomos_decision_id']);
  IF p_command->>'deployment'<>bound_deployment THEN RAISE EXCEPTION 'external action deployment is not bound to session_user' USING ERRCODE='28000'; END IF;
  IF p_command->>'service_mode' NOT IN ('normal','restore_only') OR p_command->>'quarantine_state' NOT IN ('clear','blocked') THEN RAISE EXCEPTION 'restore final mode is not allowed' USING ERRCODE='22023'; END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended('okg.external_action.deployment:'||bound_deployment,0));
  PERFORM okg.external_action_nomos_authority_snapshot(bound_deployment,p_command->>'authentication_audit_id',p_command->>'nomos_decision_id','external_action.restore_reconcile',p_command->>'request_hash');
  SELECT * INTO session_row FROM okg.external_action_restore_sessions WHERE deployment_name=bound_deployment AND restore_id=p_command->>'restore_id' FOR UPDATE;
  IF session_row.restore_session_row_id IS NULL THEN RAISE EXCEPTION 'restore session is not finalizable' USING ERRCODE='40001'; END IF;
  SELECT * INTO completion_row FROM okg.external_action_restore_completion_receipts WHERE deployment_name=bound_deployment AND restore_id=session_row.restore_id FOR SHARE;
  IF completion_row.completion_receipt_row_id IS NOT NULL THEN
    IF completion_row.completion_receipt_envelope_digest<>p_command->>'completion_receipt_envelope_digest' THEN RAISE EXCEPTION 'restore finalization id was reused with a different completion receipt' USING ERRCODE='40001'; END IF;
    RETURN jsonb_build_object('replay',TRUE,'deployment',bound_deployment,'restore_id',session_row.restore_id,'status','complete','completion_receipt_envelope_digest',completion_row.completion_receipt_envelope_digest); END IF;
  SELECT * INTO attestation_row FROM okg.external_action_restore_attestations WHERE deployment_name=bound_deployment AND envelope_digest=session_row.restore_attestation_envelope_digest FOR SHARE;
  IF attestation_row.attestation_row_id IS NULL OR attestation_row.new_epoch<>(p_command->>'new_epoch')::bigint OR attestation_row.new_epoch_lease_digest<>p_command->>'new_epoch_lease_digest' THEN RAISE EXCEPTION 'restore attestation does not match final fence' USING ERRCODE='40001'; END IF;
  SELECT * INTO fence_row FROM okg.external_action_restore_fences WHERE deployment_name=bound_deployment FOR UPDATE;
  IF fence_row.deployment_name IS NOT NULL AND fence_row.highest_accepted_epoch>=attestation_row.new_epoch THEN RAISE EXCEPTION 'restore final epoch is stale' USING ERRCODE='40001'; END IF;
  INSERT INTO okg.external_action_restore_fences (deployment_name,highest_accepted_epoch,lease_digest,quarantine_state,service_mode,lease_chain_digest,verification_config_digest,verification_login_binding_digest,restore_attestation_digest,accepted_at,mode_changed_at,store_record) VALUES (bound_deployment,attestation_row.new_epoch,p_command->>'new_epoch_lease_digest',p_command->>'quarantine_state',p_command->>'service_mode',p_command->>'lease_chain_digest',p_command->>'verification_config_digest',p_command->>'verification_login_binding_digest',attestation_row.envelope_digest,observed_at,observed_at,p_command) ON CONFLICT (deployment_name) DO UPDATE SET highest_accepted_epoch=EXCLUDED.highest_accepted_epoch,lease_digest=EXCLUDED.lease_digest,quarantine_state=EXCLUDED.quarantine_state,service_mode=EXCLUDED.service_mode,lease_chain_digest=EXCLUDED.lease_chain_digest,verification_config_digest=EXCLUDED.verification_config_digest,verification_login_binding_digest=EXCLUDED.verification_login_binding_digest,restore_attestation_digest=EXCLUDED.restore_attestation_digest,accepted_at=EXCLUDED.accepted_at,mode_changed_at=EXCLUDED.mode_changed_at,store_record=EXCLUDED.store_record;
  INSERT INTO okg.external_action_restore_completion_receipts (deployment_name,restore_id,restore_attestation_envelope_digest,completion_receipt_envelope_digest,completed_at,authentication_audit_id,nomos_decision_id,store_record) VALUES (bound_deployment,session_row.restore_id,session_row.restore_attestation_envelope_digest,p_command->>'completion_receipt_envelope_digest',observed_at,p_command->>'authentication_audit_id',p_command->>'nomos_decision_id',p_command);
  RETURN jsonb_build_object('replay',FALSE,'deployment',bound_deployment,'restore_id',session_row.restore_id,'status','complete','highest_accepted_epoch',attestation_row.new_epoch,'restore_attestation_envelope_digest',attestation_row.envelope_digest);
END
$external_action_restore_finalize$;
CREATE OR REPLACE FUNCTION okg.external_action_store_snapshot()
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, okg AS $external_action_store_snapshot_v2$
DECLARE bound_deployment TEXT:=okg.external_action_session_deployment();
BEGIN
  PERFORM okg.external_action_verifier_capability_status();
  PERFORM pg_advisory_xact_lock(hashtextextended('okg.external_action.deployment:'||bound_deployment,0));
  RETURN jsonb_build_object(
    'execution_epoch',COALESCE((SELECT max(epoch) FROM okg.external_action_execution_epoch_leases WHERE deployment_name=bound_deployment),0),
    'execution_epoch_lease_digest',(SELECT envelope_digest FROM okg.external_action_execution_epoch_leases WHERE deployment_name=bound_deployment ORDER BY epoch DESC,lease_sequence DESC LIMIT 1),
    'proposals',COALESCE((SELECT jsonb_agg(jsonb_build_object('key',p.proposal_id,'value',COALESCE(p.store_record,okg.external_action_logical_json_record(to_jsonb(p))-ARRAY['store_record','action_contract_record','submission_idempotency_record']::text[])) ORDER BY p.proposal_id) FROM okg.external_action_proposals p WHERE p.deployment_name=bound_deployment),'[]'::jsonb),
    'payload_versions',COALESCE((SELECT jsonb_agg(jsonb_build_object('bucket',v.proposal_id,'value',COALESCE(v.store_record,okg.external_action_logical_json_record(to_jsonb(v))-'store_record')||jsonb_build_object('version',v.payload_version,'difference',v.difference_field_codes,'canonical_payload',jsonb_build_object('__okg_bytes_b64',replace(encode(v.canonical_payload,'base64'),E'\n','')))) ORDER BY v.proposal_id,v.payload_version) FROM okg.external_action_payload_versions v WHERE v.deployment_name=bound_deployment),'[]'::jsonb),
    'auth_audits',COALESCE((SELECT jsonb_agg(jsonb_build_object('key',a.authentication_audit_id,'value',COALESCE(a.store_record,okg.external_action_logical_json_record(to_jsonb(a))-'store_record')) ORDER BY a.authentication_audit_id) FROM okg.external_action_auth_audits a WHERE a.deployment_name=bound_deployment),'[]'::jsonb),
    'decisions',COALESCE((SELECT jsonb_agg(jsonb_build_object('bucket',d.proposal_id,'value',COALESCE(d.store_record,jsonb_build_object('decision_id',d.decision_id,'proposal_id',d.proposal_id,'verdict',CASE WHEN d.verdict='approved' THEN 'approve' ELSE 'reject' END,'lifecycle_version',d.approved_lifecycle_version,'payload_version',d.approved_payload_version,'payload_hash',d.approved_payload_hash,'approver_principal',d.approver_principal,'approver_separation_subject',d.approver_separation_subject,'policy_digest',d.policy_digest,'execute_before',d.execute_before,'reason_code',d.reason_code,'decision_binding_hash',d.decision_binding_hash,'created_at',d.created_at))) ORDER BY d.proposal_id,d.decision_row_id) FROM okg.external_action_decisions d WHERE d.deployment_name=bound_deployment),'[]'::jsonb),
    'transition_idempotency',COALESCE((SELECT jsonb_agg(jsonb_build_object('key',jsonb_build_array(t.deployment_name,t.proposal_id,t.actor_separation_subject,t.idempotency_key_hash),'value',COALESCE(t.store_record,okg.external_action_logical_json_record(to_jsonb(t))-'store_record')) ORDER BY t.transition_idempotency_id) FROM okg.external_action_transition_idempotency t WHERE t.deployment_name=bound_deployment),'[]'::jsonb),
    'executions',COALESCE((SELECT jsonb_agg(jsonb_build_object('key',e.proposal_id,'value',COALESCE(e.store_record,okg.external_action_logical_json_record(to_jsonb(e))-'store_record')) ORDER BY e.execution_id) FROM okg.external_action_executions e WHERE e.deployment_name=bound_deployment),'[]'::jsonb),
    'outcomes',COALESCE((SELECT jsonb_agg(jsonb_build_object('key',o.execution_id,'value',COALESCE(o.store_record,okg.external_action_logical_json_record(to_jsonb(o))-'store_record')) ORDER BY o.outcome_id) FROM okg.external_action_outcomes o WHERE o.deployment_name=bound_deployment),'[]'::jsonb),
    'events',COALESCE((SELECT jsonb_agg(jsonb_build_object('bucket',e.effect_instance_key,'value',COALESCE(e.store_record,okg.external_action_logical_json_record(to_jsonb(e))-'store_record')) ORDER BY e.effect_instance_key,e.sequence) FROM okg.external_action_events e WHERE e.deployment_name=bound_deployment),'[]'::jsonb),
    'attestations',COALESCE((SELECT jsonb_agg(jsonb_build_object('key',a.envelope_digest,'value',COALESCE(a.store_record,okg.external_action_logical_json_record(to_jsonb(a))-'store_record')) ORDER BY a.attestation_id) FROM okg.external_action_owner_unavailability_attestations a WHERE a.deployment_name=bound_deployment),'[]'::jsonb),
    'contracts',COALESCE((SELECT jsonb_agg(jsonb_build_object('key',p.action_contract_digest,'value',p.action_contract_record) ORDER BY p.action_contract_digest) FROM (SELECT DISTINCT ON (action_contract_digest) action_contract_digest,action_contract_record FROM okg.external_action_proposals WHERE deployment_name=bound_deployment AND action_contract_record IS NOT NULL ORDER BY action_contract_digest,proposal_id) p),'[]'::jsonb),
    'leases',COALESCE((SELECT jsonb_agg(jsonb_build_object('bucket',l.deployment_name,'value',COALESCE(l.store_record,okg.external_action_logical_json_record(to_jsonb(l))-'store_record')) ORDER BY l.epoch,l.lease_sequence) FROM okg.external_action_execution_epoch_leases l WHERE l.deployment_name=bound_deployment),'[]'::jsonb),
    'restore_fences',COALESCE((SELECT jsonb_agg(jsonb_build_object('key',f.deployment_name,'value',COALESCE(f.store_record,okg.external_action_logical_json_record(to_jsonb(f))-'store_record'))) FROM okg.external_action_restore_fences f WHERE f.deployment_name=bound_deployment),'[]'::jsonb),
    'restore_attestations',COALESCE((SELECT jsonb_agg(jsonb_build_object('key',a.restore_id,'value',COALESCE(a.store_record,okg.external_action_logical_json_record(to_jsonb(a))-'store_record')) ORDER BY a.attestation_row_id) FROM okg.external_action_restore_attestations a WHERE a.deployment_name=bound_deployment),'[]'::jsonb),
    'restore_sessions',COALESCE((SELECT jsonb_agg(jsonb_build_object('key',s.restore_id,'value',COALESCE(s.store_record,okg.external_action_logical_json_record(to_jsonb(s))-'store_record')) ORDER BY s.restore_session_row_id) FROM okg.external_action_restore_sessions s WHERE s.deployment_name=bound_deployment),'[]'::jsonb),
    'restore_completion_receipts',COALESCE((SELECT jsonb_agg(jsonb_build_object('key',r.restore_id,'value',COALESCE(r.store_record,okg.external_action_logical_json_record(to_jsonb(r))-'store_record')) ORDER BY r.completion_receipt_row_id) FROM okg.external_action_restore_completion_receipts r WHERE r.deployment_name=bound_deployment),'[]'::jsonb),
    'restore_checkpoint_staging',COALESCE((SELECT jsonb_agg(jsonb_build_object('key',jsonb_build_array(c.restore_id,c.batch_index),'value',COALESCE(c.store_record,okg.external_action_logical_json_record(to_jsonb(c))-'store_record')) ORDER BY c.checkpoint_staging_row_id) FROM okg.external_action_restore_checkpoint_staging c WHERE c.deployment_name=bound_deployment),'[]'::jsonb),
    'restore_checkpoint_entries',COALESCE((SELECT jsonb_agg(jsonb_build_object('key',jsonb_build_array(c.restore_id,c.batch_index,c.ordinal),'value',COALESCE(c.checkpoint_payload,jsonb_build_object('effect_instance_key',c.effect_instance_key,'checkpoint_envelope_digest',c.checkpoint_envelope_digest,'proof_digest',c.proof_digest))) ORDER BY c.checkpoint_entry_row_id) FROM okg.external_action_restore_checkpoint_entries c WHERE c.deployment_name=bound_deployment),'[]'::jsonb),
    'restore_finalize_idempotency',COALESCE((SELECT jsonb_agg(jsonb_build_object('key',r.restore_id,'value',jsonb_build_object('operator_separation_subject',r.operator_separation_subject,'idempotency_key_hash',r.idempotency_key_hash,'request_hash',r.request_hash,'response_body_jcs',jsonb_build_object('__okg_bytes_b64',encode(r.response_body_jcs,'base64')),'completion_receipt_envelope_digest',r.completion_receipt_envelope_digest)) ORDER BY r.finalize_idempotency_row_id) FROM okg.external_action_restore_finalize_idempotency r WHERE r.deployment_name=bound_deployment),'[]'::jsonb),
    'restore_recoveries',COALESCE((SELECT jsonb_agg(jsonb_build_object('key',jsonb_build_array(r.effect_instance_key,r.signed_worker_checkpoint_digest),'value',COALESCE(r.store_record,okg.external_action_logical_json_record(to_jsonb(r))-'store_record')) ORDER BY r.recovery_row_id) FROM okg.external_action_restore_recoveries r WHERE r.deployment_name=bound_deployment),'[]'::jsonb),
    'dpop_replay_nonces',COALESCE((SELECT jsonb_agg(jsonb_build_object('key',jsonb_build_array(n.issuer_digest,n.sender_key_thumbprint,n.jti_digest),'value',okg.external_action_logical_json_record(to_jsonb(n))) ORDER BY n.dpop_replay_nonce_id) FROM okg.external_action_dpop_replay_nonces n WHERE n.deployment_name=bound_deployment),'[]'::jsonb)
  );
END
$external_action_store_snapshot_v2$;

CREATE OR REPLACE FUNCTION okg.external_action_authorized_read(p_command JSONB)
RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $external_action_authorized_read$
DECLARE
  bound_deployment TEXT := okg.external_action_session_deployment();
  route_operation TEXT := p_command ->> 'operation';
  policy_action TEXT;
  proposal_row okg.external_action_proposals%ROWTYPE;
  payload_row okg.external_action_payload_versions%ROWTYPE;
  proposal_projection JSONB;
  proposals_projection JSONB;
  events_projection JSONB;
  recovery_projection JSONB;
  expiring RECORD;
BEGIN
  PERFORM okg.external_action_verifier_capability_status();
  PERFORM okg.external_action_require_command(
    p_command,
    ARRAY[
      'deployment','operation','request_hash','include_payload','resource','proposal_id',
      'effect_instance_key','state','limit','authentication_audit_id',
      'nomos_decision_id','policy_digest'
    ],
    ARRAY[
      'deployment','operation','request_hash','include_payload','resource',
      'authentication_audit_id','nomos_decision_id'
    ]
  );
  IF p_command ->> 'deployment' <> bound_deployment THEN
    RAISE EXCEPTION 'external action deployment is not bound to session_user'
      USING ERRCODE='28000';
  END IF;
  IF route_operation NOT IN (
       'read','read_payload','list','timeline','recovery_record'
     )
     OR jsonb_typeof(p_command -> 'include_payload') IS DISTINCT FROM 'boolean'
     OR (p_command ->> 'include_payload')::boolean IS DISTINCT FROM
          (route_operation = 'read_payload')
     OR p_command ->> 'resource' IS DISTINCT FROM (
          CASE route_operation
            WHEN 'recovery_record' THEN p_command ->> 'effect_instance_key'
            WHEN 'list' THEN bound_deployment
            ELSE p_command ->> 'proposal_id'
          END
        )
     OR p_command ->> 'request_hash' !~ '^sha256:[0-9a-f]{64}$' THEN
    RAISE EXCEPTION 'external action read request is invalid'
      USING ERRCODE='22023';
  END IF;
  IF route_operation IN ('read','read_payload','timeline')
     AND NULLIF(p_command ->> 'proposal_id','') IS NULL THEN
    RAISE EXCEPTION 'external action read proposal id is unavailable'
      USING ERRCODE='22023';
  END IF;
  IF route_operation = 'recovery_record'
     AND NULLIF(p_command ->> 'effect_instance_key','') IS NULL THEN
    RAISE EXCEPTION 'external action recovery key is unavailable'
      USING ERRCODE='22023';
  END IF;
  IF route_operation = 'list'
     AND p_command ->> 'state' IS NOT NULL
     AND p_command ->> 'state' NOT IN (
       'pending','edited','approved','rejected','expired','cancelled',
       'unavailable','ready','claimed','succeeded','failed','indeterminate'
     ) THEN
    RAISE EXCEPTION 'external action list state is invalid'
      USING ERRCODE='22023';
  END IF;
  IF p_command -> 'limit' <> 'null'::jsonb
     AND (jsonb_typeof(p_command -> 'limit') IS DISTINCT FROM 'number'
          OR (p_command ->> 'limit')::integer NOT BETWEEN 1 AND 1000) THEN
    RAISE EXCEPTION 'external action list limit is invalid'
      USING ERRCODE='22023';
  END IF;
  policy_action := CASE WHEN route_operation = 'read_payload'
    THEN 'external_action.read_payload' ELSE 'external_action.read' END;
  PERFORM pg_advisory_xact_lock(
    hashtextextended('okg.external_action.deployment:' || bound_deployment,0)
  );
  PERFORM okg.external_action_nomos_authority_snapshot(
    bound_deployment,p_command->>'authentication_audit_id',
    p_command->>'nomos_decision_id',policy_action,p_command->>'request_hash'
  );

  IF route_operation IN ('read','read_payload','timeline')
     AND EXISTS (
       SELECT 1
         FROM okg.external_action_proposals
        WHERE deployment_name=bound_deployment
          AND proposal_id=p_command->>'proposal_id'
     ) THEN
    PERFORM okg.external_action_materialize_expiry(p_command ->> 'proposal_id');
  ELSIF route_operation = 'list' THEN
    FOR expiring IN
      SELECT proposal_id
        FROM okg.external_action_proposals
       WHERE deployment_name=bound_deployment
         AND review_state IN ('pending','edited')
         AND effective_review_expires_at <= clock_timestamp()
       ORDER BY proposal_id
    LOOP
      PERFORM okg.external_action_materialize_expiry(expiring.proposal_id);
    END LOOP;
  END IF;

  IF route_operation IN ('read','read_payload') THEN
    SELECT * INTO proposal_row
      FROM okg.external_action_proposals
     WHERE deployment_name=bound_deployment
       AND proposal_id=p_command->>'proposal_id'
     FOR SHARE;
    IF proposal_row.proposal_id IS NULL THEN
      RETURN jsonb_build_object('proposal',NULL);
    END IF;
    proposal_projection := jsonb_build_object(
      'proposal_id',proposal_row.proposal_id,
      'deployment',proposal_row.deployment_name,
      'review_state',proposal_row.review_state,
      'execution_state',proposal_row.execution_state,
      'proposer_principal',proposal_row.store_record->>'proposer_principal',
      'proposer_separation_subject',proposal_row.proposer_separation_subject,
      'lifecycle_version',proposal_row.lifecycle_version,
      'payload_version',proposal_row.payload_version,
      'execution_epoch',proposal_row.execution_epoch,
      'effect_instance_key',proposal_row.effect_instance_key,
      'submission_content_hash',proposal_row.submission_content_hash,
      'original_payload_hash',proposal_row.original_payload_hash,
      'current_payload_hash',proposal_row.current_payload_hash,
      'proposal_request_hash',proposal_row.submission_request_hash,
      'automation_id',proposal_row.automation_id,
      'automation_version',proposal_row.automation_version,
      'run_id',proposal_row.run_id,
      'correlation_id',proposal_row.correlation_id,
      'action_type',proposal_row.action_type,
      'target_ref',proposal_row.target_ref,
      'risk_tier',proposal_row.risk_tier,
      'action_schema_digest',proposal_row.action_schema_digest,
      'action_contract_digest',proposal_row.action_contract_digest,
      'executor_contract_digest',proposal_row.executor_contract_digest,
      'payload_renderer_digest',proposal_row.payload_renderer_digest,
      'governed_tool_binding_digest',proposal_row.governed_tool_binding_digest,
      'provider_connection_contract_digest',proposal_row.provider_connection_contract_digest,
      'provider_capability_digest',proposal_row.provider_capability_digest,
      'provider_operation_id',proposal_row.provider_operation_id,
      'provider_operation_descriptor_digest',proposal_row.provider_operation_descriptor_digest,
      'requested_review_expires_at',to_char(
        proposal_row.requested_review_expires_at AT TIME ZONE 'UTC',
        'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
      ),
      'effective_review_expires_at',to_char(
        proposal_row.effective_review_expires_at AT TIME ZONE 'UTC',
        'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
      ),
      'summary_code',proposal_row.summary_code,
      'evidence_digests',proposal_row.evidence_digests,
      'decision_id',proposal_row.current_decision_id,
      'decision_binding_hash',proposal_row.current_decision_binding_hash,
      'execute_before',CASE WHEN proposal_row.execute_before IS NULL THEN NULL
        ELSE to_char(proposal_row.execute_before AT TIME ZONE 'UTC',
          'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') END,
      'policy_digest',proposal_row.store_record->>'policy_digest',
      'authentication_audit_id',proposal_row.store_record->>'authentication_audit_id',
      'nomos_decision_id',proposal_row.store_record->>'nomos_decision_id',
      'created_at',to_char(proposal_row.created_at AT TIME ZONE 'UTC',
        'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
      'updated_at',to_char(proposal_row.updated_at AT TIME ZONE 'UTC',
        'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
      'etag',format('"lifecycle:%s;payload-version:%s;payload:%s"',
        proposal_row.lifecycle_version,proposal_row.payload_version,
        proposal_row.current_payload_hash)
    );
    IF route_operation = 'read_payload' THEN
      SELECT * INTO payload_row
        FROM okg.external_action_payload_versions
       WHERE deployment_name=bound_deployment
         AND proposal_id=proposal_row.proposal_id
         AND payload_version=proposal_row.payload_version
         AND payload_hash=proposal_row.current_payload_hash
       FOR SHARE;
      IF payload_row.payload_version_id IS NULL THEN
        RAISE EXCEPTION 'external action payload is unavailable'
          USING ERRCODE='40001';
      END IF;
      proposal_projection := proposal_projection || jsonb_build_object(
        'canonical_payload',jsonb_build_object(
          '__okg_bytes_b64',replace(encode(payload_row.canonical_payload,'base64'),E'\n','')
        )
      );
    END IF;
    RETURN jsonb_build_object('proposal',proposal_projection);
  END IF;

  IF route_operation = 'list' THEN
    SELECT COALESCE(jsonb_agg(projection ORDER BY projection->>'proposal_id'),'[]'::jsonb)
      INTO proposals_projection
      FROM (
        SELECT jsonb_build_object(
          'proposal_id',p.proposal_id,'deployment',p.deployment_name,
          'review_state',p.review_state,'execution_state',p.execution_state,
          'proposer_principal',p.store_record->>'proposer_principal',
          'proposer_separation_subject',p.proposer_separation_subject,
          'lifecycle_version',p.lifecycle_version,'payload_version',p.payload_version,
          'execution_epoch',p.execution_epoch,'effect_instance_key',p.effect_instance_key,
          'submission_content_hash',p.submission_content_hash,
          'original_payload_hash',p.original_payload_hash,
          'current_payload_hash',p.current_payload_hash,
          'proposal_request_hash',p.submission_request_hash,
          'action_type',p.action_type,'target_ref',p.target_ref,'risk_tier',p.risk_tier,
          'action_schema_digest',p.action_schema_digest,
          'action_contract_digest',p.action_contract_digest,
          'executor_contract_digest',p.executor_contract_digest,
          'payload_renderer_digest',p.payload_renderer_digest,
          'governed_tool_binding_digest',p.governed_tool_binding_digest,
          'provider_connection_contract_digest',p.provider_connection_contract_digest,
          'provider_capability_digest',p.provider_capability_digest,
          'provider_operation_id',p.provider_operation_id,
          'provider_operation_descriptor_digest',p.provider_operation_descriptor_digest,
          'requested_review_expires_at',to_char(p.requested_review_expires_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
          'effective_review_expires_at',to_char(p.effective_review_expires_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
          'summary_code',p.summary_code,'evidence_digests',p.evidence_digests,
          'decision_id',p.current_decision_id,
          'decision_binding_hash',p.current_decision_binding_hash,
          'execute_before',CASE WHEN p.execute_before IS NULL THEN NULL ELSE to_char(p.execute_before AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"') END,
          'policy_digest',p.store_record->>'policy_digest',
          'authentication_audit_id',p.store_record->>'authentication_audit_id',
          'nomos_decision_id',p.store_record->>'nomos_decision_id',
          'created_at',to_char(p.created_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
          'updated_at',to_char(p.updated_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
          'etag',format('"lifecycle:%s;payload-version:%s;payload:%s"',p.lifecycle_version,p.payload_version,p.current_payload_hash)
        ) AS projection
          FROM okg.external_action_proposals p
         WHERE p.deployment_name=bound_deployment
           AND (p_command->>'state' IS NULL
                OR p.review_state=p_command->>'state'
                OR p.execution_state=p_command->>'state')
         ORDER BY p.proposal_id
         LIMIT COALESCE((p_command->>'limit')::integer,1000)
      ) AS rows;
    RETURN jsonb_build_object('proposals',proposals_projection);
  END IF;

  IF route_operation = 'timeline' THEN
    SELECT * INTO proposal_row
      FROM okg.external_action_proposals
     WHERE deployment_name=bound_deployment
       AND proposal_id=p_command->>'proposal_id'
     FOR SHARE;
    IF proposal_row.proposal_id IS NULL THEN
      RETURN jsonb_build_object('events',NULL);
    END IF;
    SELECT COALESCE(jsonb_agg(
             COALESCE(e.store_record->'event_projection',okg.external_action_logical_json_record(to_jsonb(e))-ARRAY['event_id','deployment_name','store_record']::text[])
             || jsonb_build_object('event_hash',e.event_hash)
             ORDER BY e.sequence
           ),'[]'::jsonb)
      INTO events_projection
      FROM okg.external_action_events e
     WHERE e.deployment_name=bound_deployment
       AND e.effect_instance_key=proposal_row.effect_instance_key;
    RETURN jsonb_build_object('events',events_projection);
  END IF;

  SELECT okg.external_action_logical_json_record(to_jsonb(r))-ARRAY['recovery_row_id','store_record']::text[]
    INTO recovery_projection
    FROM okg.external_action_restore_recoveries r
   WHERE r.deployment_name=bound_deployment
     AND r.effect_instance_key=p_command->>'effect_instance_key'
   ORDER BY r.new_epoch DESC,r.recorded_at DESC,r.recovery_row_id DESC
   LIMIT 1;
  RETURN jsonb_build_object('recovery_record',recovery_projection);
END
$external_action_authorized_read$;
REVOKE ALL ON FUNCTION okg.external_action_authorized_read(JSONB) FROM PUBLIC;

CREATE OR REPLACE FUNCTION okg.external_action_store_startup(p_state JSONB)
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, okg AS $external_action_store_startup_v2$
DECLARE
  bound_deployment TEXT:=okg.external_action_session_deployment();
  fence_row okg.external_action_restore_fences%ROWTYPE;
  lease_row okg.external_action_execution_epoch_leases%ROWTYPE;
  lease_count BIGINT;
  derived_mode TEXT;
  derived_quarantine TEXT;
  epoch_advanced BOOLEAN;
BEGIN
  PERFORM okg.external_action_verifier_capability_status();
  -- Mode and quarantine are database decisions.  Keep the legacy fields in
  -- the allow-list for adapter compatibility, but never read them.
  PERFORM okg.external_action_require_command(p_state,ARRAY['deployment','highest_accepted_epoch','lease_digest','service_mode','quarantine_state','lease_chain_digest','verification_config_digest','verification_login_binding_digest'],ARRAY['deployment','highest_accepted_epoch','lease_digest','lease_chain_digest','verification_config_digest','verification_login_binding_digest']);
  IF p_state->>'deployment'<>bound_deployment THEN RAISE EXCEPTION 'external action deployment is not bound to session_user' USING ERRCODE='28000'; END IF;
  IF (p_state->>'highest_accepted_epoch')::bigint < 0 THEN
    RAISE EXCEPTION 'external action startup epoch is invalid' USING ERRCODE='22023';
  END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended('okg.external_action.deployment:'||bound_deployment,0));
  SELECT * INTO fence_row FROM okg.external_action_restore_fences WHERE deployment_name=bound_deployment FOR UPDATE;
  SELECT * INTO lease_row
    FROM okg.external_action_execution_epoch_leases
   WHERE deployment_name=bound_deployment
   ORDER BY epoch DESC, lease_sequence DESC
   LIMIT 1;
  SELECT count(*) INTO lease_count
    FROM okg.external_action_execution_epoch_leases
   WHERE deployment_name=bound_deployment;
  IF lease_row.lease_row_id IS NULL
     OR lease_row.epoch<>(p_state->>'highest_accepted_epoch')::bigint
     OR lease_row.envelope_digest<>p_state->>'lease_digest'
     OR lease_row.lease_chain_digest IS DISTINCT FROM p_state->>'lease_chain_digest'
     OR lease_row.verification_config_digest<>p_state->>'verification_config_digest'
     OR lease_row.verification_login_binding_digest<>p_state->>'verification_login_binding_digest' THEN
    RAISE EXCEPTION 'startup fence does not match the installed verified lease'
      USING ERRCODE='40001';
  END IF;
  IF fence_row.deployment_name IS NULL THEN
    -- A lone genesis lease is safe on first use even when its epoch is
    -- nonzero.  Any retained suffix requires restore-only quarantine.
    derived_mode := CASE WHEN lease_count=1 AND lease_row.lease_sequence=1
                              AND lease_row.previous_lease_digest IS NULL
                              AND lease_row.previous_epoch_digest IS NULL
                         THEN 'normal' ELSE 'restore_only' END;
    derived_quarantine := CASE WHEN derived_mode='normal' THEN 'clear' ELSE 'blocked' END;
    INSERT INTO okg.external_action_restore_fences (deployment_name,highest_accepted_epoch,lease_digest,quarantine_state,service_mode,lease_chain_digest,verification_config_digest,verification_login_binding_digest,accepted_at,mode_changed_at,store_record)
    VALUES (bound_deployment,lease_row.epoch,lease_row.envelope_digest,derived_quarantine,derived_mode,lease_row.lease_chain_digest,lease_row.verification_config_digest,lease_row.verification_login_binding_digest,clock_timestamp(),clock_timestamp(),p_state);
  ELSE
    IF lease_row.epoch < fence_row.highest_accepted_epoch THEN
      RAISE EXCEPTION 'startup lease epoch is stale' USING ERRCODE='40001';
    END IF;
    epoch_advanced := lease_row.epoch > fence_row.highest_accepted_epoch;
    -- Replaying the exact installed lease is a true no-op.  In particular,
    -- do not refresh accepted_at or store_record: those columns are the
    -- durable evidence of the first acceptance and must remain stable when
    -- a restarted process re-presents the same verified lease.
    IF NOT epoch_advanced
       AND fence_row.lease_digest IS NOT DISTINCT FROM lease_row.envelope_digest
       AND fence_row.lease_chain_digest IS NOT DISTINCT FROM lease_row.lease_chain_digest
       AND fence_row.verification_config_digest IS NOT DISTINCT FROM lease_row.verification_config_digest
       AND fence_row.verification_login_binding_digest IS NOT DISTINCT FROM lease_row.verification_login_binding_digest THEN
      RETURN jsonb_build_object('replay',TRUE,'accepted',TRUE,'deployment',bound_deployment,
        'service_mode',fence_row.service_mode,'quarantine_state',fence_row.quarantine_state,
        'highest_accepted_epoch',fence_row.highest_accepted_epoch);
    END IF;
    derived_mode := CASE WHEN epoch_advanced THEN 'restore_only' ELSE fence_row.service_mode END;
    derived_quarantine := CASE WHEN epoch_advanced THEN 'blocked' ELSE fence_row.quarantine_state END;
    UPDATE okg.external_action_restore_fences
       SET highest_accepted_epoch=lease_row.epoch,
           lease_digest=lease_row.envelope_digest,
           quarantine_state=derived_quarantine,
           service_mode=derived_mode,
           lease_chain_digest=lease_row.lease_chain_digest,
           verification_config_digest=lease_row.verification_config_digest,
           verification_login_binding_digest=lease_row.verification_login_binding_digest,
           accepted_at=clock_timestamp(),
           mode_changed_at=CASE WHEN epoch_advanced THEN clock_timestamp() ELSE mode_changed_at END,
           store_record=p_state
     WHERE deployment_name=bound_deployment;
  END IF;
  RETURN jsonb_build_object('replay',fence_row.deployment_name IS NOT NULL,'accepted',TRUE,'deployment',bound_deployment,'service_mode',derived_mode,'quarantine_state',derived_quarantine,'highest_accepted_epoch',lease_row.epoch);
END
$external_action_store_startup_v2$;
CREATE OR REPLACE FUNCTION okg.external_action_store_resolve_principal(p_issuer TEXT,p_subject TEXT)
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, okg AS $external_action_store_resolve_principal_v2$
DECLARE bound_deployment TEXT:=okg.external_action_session_deployment(); matched_count INTEGER; result JSONB;
BEGIN
  PERFORM okg.external_action_verifier_capability_status();
  SELECT count(*)::integer INTO matched_count FROM okg.nomos_identity_providers p JOIN okg.nomos_subject_bindings b ON b.deployment_name=p.deployment_name AND b.provider_id=p.provider_id AND b.issuer=p.issuer WHERE p.deployment_name=bound_deployment AND p.issuer=p_issuer AND p.status='active' AND b.status='active' AND b.claim_path=p.subject_claim AND b.claim_value=p_subject AND (b.effective_from IS NULL OR b.effective_from<=clock_timestamp()) AND (b.effective_until IS NULL OR clock_timestamp()<b.effective_until);
  IF matched_count<>1 THEN RAISE EXCEPTION 'deployment-pinned principal mapping is unavailable' USING ERRCODE='28000'; END IF;
  SELECT jsonb_build_object(
    'principal',b.subject,'separation_subject',b.subject,'provider_id',p.provider_id,
    'subject_binding_id',b.binding_id,'issuer',p.issuer,'deployment',bound_deployment,
    'provider_status',p.status,'provider_updated_at',p.updated_at,
    'provider_credential_generation_digest',COALESCE(
      p.metadata->>'credential_generation_digest',
      p.metadata->>'provider_credential_generation_digest',
      p.metadata->>'credential_generation'),
    'provider_certificate_thumbprint',COALESCE(
      p.metadata->>'certificate_thumbprint',p.metadata->>'sender_key_thumbprint'),
    'provider_certificate_not_before',p.metadata->>'certificate_not_before',
    'provider_certificate_not_after',p.metadata->>'certificate_not_after',
    'subject',b.subject,'binding_status',b.status,'binding_updated_at',b.updated_at,
    'binding_credential_generation_digest',COALESCE(
      b.metadata->>'credential_generation_digest',
      b.metadata->>'binding_credential_generation_digest',
      b.metadata->>'credential_generation'),
    'binding_certificate_thumbprint',COALESCE(
      b.metadata->>'certificate_thumbprint',b.metadata->>'sender_key_thumbprint'),
    'binding_certificate_not_before',b.metadata->>'certificate_not_before',
    'binding_certificate_not_after',b.metadata->>'certificate_not_after',
    'certificate_not_before',COALESCE(
      b.metadata->>'certificate_not_before',p.metadata->>'certificate_not_before'),
    'certificate_not_after',COALESCE(
      b.metadata->>'certificate_not_after',p.metadata->>'certificate_not_after')
  ) INTO result FROM okg.nomos_identity_providers p JOIN okg.nomos_subject_bindings b ON b.deployment_name=p.deployment_name AND b.provider_id=p.provider_id AND b.issuer=p.issuer WHERE p.deployment_name=bound_deployment AND p.issuer=p_issuer AND p.status='active' AND b.status='active' AND b.claim_path=p.subject_claim AND b.claim_value=p_subject AND (b.effective_from IS NULL OR b.effective_from<=clock_timestamp()) AND (b.effective_until IS NULL OR clock_timestamp()<b.effective_until);
  RETURN result;
END
$external_action_store_resolve_principal_v2$;

-- Persist the verified startup lease through one fixed, closed command.  This
-- is the pre-bind suffix path: no raw table grant or relation name is exposed.
CREATE OR REPLACE FUNCTION okg.external_action_store_insert_lease(p_command JSONB)
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $external_action_store_insert_lease$
DECLARE
  bound_deployment TEXT := okg.external_action_session_deployment();
  lease_row okg.external_action_execution_epoch_leases%ROWTYPE;
  prior_row okg.external_action_execution_epoch_leases%ROWTYPE;
  prior_epoch_row okg.external_action_execution_epoch_leases%ROWTYPE;
  inserted_count INTEGER;
  existing_lease_count BIGINT;
BEGIN
  PERFORM okg.external_action_verifier_capability_status();
  PERFORM okg.external_action_require_command(
    p_command,
    ARRAY['deployment','epoch','lease_sequence','previous_lease_digest',
      'previous_epoch_digest','issued_at','expires_at',
      'signer_separation_subject','nonce','protected_b64','payload_b64',
      'signature_b64','signer_key_id_digest','envelope_digest',
      'verification_source','verification_config_digest',
      'verification_login_binding_digest','lease_chain_digest',
      'authentication_audit_id','nomos_decision_id'],
    ARRAY['deployment','epoch','lease_sequence','issued_at','expires_at',
      'signer_separation_subject','nonce','protected_b64','payload_b64',
      'signature_b64','envelope_digest','verification_source',
      'verification_config_digest','verification_login_binding_digest']
  );
  IF p_command->>'deployment' <> bound_deployment THEN
    RAISE EXCEPTION 'external action deployment is not bound to session_user'
      USING ERRCODE='28000';
  END IF;
  IF NOT (p_command ? 'previous_lease_digest')
     OR NOT (p_command ? 'previous_epoch_digest') THEN
    RAISE EXCEPTION 'external action lease predecessor fields are required'
      USING ERRCODE='22023';
  END IF;
  IF p_command->>'signer_separation_subject' = ''
     OR p_command->>'nonce' = '' OR p_command->>'protected_b64' = ''
     OR p_command->>'payload_b64' = '' OR p_command->>'signature_b64' = ''
     OR p_command->>'signer_key_id_digest' = ''
     OR p_command->>'envelope_digest' = ''
     OR p_command->>'verification_config_digest' = ''
     OR p_command->>'verification_login_binding_digest' = '' THEN
    RAISE EXCEPTION 'external action lease contains a blank field'
      USING ERRCODE='22023';
  END IF;
  IF p_command->>'verification_source' NOT IN
       ('signed_startup_gate','strict_authenticated_api') THEN
    RAISE EXCEPTION 'external action lease verification source is not allowed'
      USING ERRCODE='22023';
  END IF;
  IF p_command->>'verification_source' = 'signed_startup_gate'
     AND (p_command->>'lease_chain_digest' IS NULL
          OR p_command->>'authentication_audit_id' IS NOT NULL
          OR p_command->>'nomos_decision_id' IS NOT NULL) THEN
    RAISE EXCEPTION 'startup lease authority fields are inconsistent'
      USING ERRCODE='22023';
  ELSIF p_command->>'verification_source' = 'strict_authenticated_api'
        AND (p_command->>'lease_chain_digest' IS NOT NULL
             OR p_command->>'authentication_audit_id' IS NULL
             OR p_command->>'nomos_decision_id' IS NULL) THEN
    RAISE EXCEPTION 'authenticated lease authority fields are inconsistent'
      USING ERRCODE='22023';
  END IF;
  IF (p_command->>'issued_at')::timestamptz >=
       (p_command->>'expires_at')::timestamptz
     OR (p_command->>'epoch')::bigint < 0
     OR (p_command->>'lease_sequence')::bigint < 1 THEN
    RAISE EXCEPTION 'external action lease interval or sequence is invalid'
      USING ERRCODE='22023';
  END IF;
  PERFORM pg_advisory_xact_lock(
    hashtextextended('okg.external_action.deployment:'||bound_deployment,0)
  );
  SELECT count(*) INTO existing_lease_count
    FROM okg.external_action_execution_epoch_leases
   WHERE deployment_name=bound_deployment;
  IF p_command->>'verification_source' = 'strict_authenticated_api' THEN
    PERFORM okg.external_action_nomos_authority_snapshot(
      bound_deployment,p_command->>'authentication_audit_id',
      p_command->>'nomos_decision_id','external_action.execute',
      p_command->>'envelope_digest');
  END IF;

  -- Idempotency is checked before chain guards.  Retries return the exact
  -- stored evidence and do not re-evaluate a historical lease boundary.
  SELECT * INTO lease_row
    FROM okg.external_action_execution_epoch_leases
   WHERE deployment_name=bound_deployment
     AND envelope_digest=p_command->>'envelope_digest'
   FOR UPDATE;
  IF lease_row.lease_row_id IS NOT NULL THEN
    IF lease_row.epoch <> (p_command->>'epoch')::bigint
       OR lease_row.lease_sequence <> (p_command->>'lease_sequence')::bigint
       OR lease_row.previous_lease_digest IS DISTINCT FROM p_command->>'previous_lease_digest'
       OR lease_row.previous_epoch_digest IS DISTINCT FROM p_command->>'previous_epoch_digest'
       OR lease_row.issued_at <> (p_command->>'issued_at')::timestamptz
       OR lease_row.expires_at <> (p_command->>'expires_at')::timestamptz
       OR lease_row.signer_separation_subject <> p_command->>'signer_separation_subject'
       OR lease_row.nonce <> p_command->>'nonce'
       OR lease_row.protected_b64 <> p_command->>'protected_b64'
       OR lease_row.payload_b64 <> p_command->>'payload_b64'
       OR lease_row.signature_b64 <> p_command->>'signature_b64'
       OR lease_row.signer_key_id_digest IS DISTINCT FROM p_command->>'signer_key_id_digest'
       OR lease_row.verification_source <> p_command->>'verification_source'
       OR lease_row.verification_config_digest <> p_command->>'verification_config_digest'
       OR lease_row.verification_login_binding_digest <> p_command->>'verification_login_binding_digest'
       OR lease_row.lease_chain_digest IS DISTINCT FROM p_command->>'lease_chain_digest'
       OR lease_row.authentication_audit_id IS DISTINCT FROM p_command->>'authentication_audit_id'
       OR lease_row.nomos_decision_id IS DISTINCT FROM p_command->>'nomos_decision_id' THEN
      RAISE EXCEPTION 'external action lease id was reused with a different request'
        USING ERRCODE='40001';
    END IF;
    RETURN jsonb_build_object('replay',TRUE,'deployment',bound_deployment,
      'epoch',lease_row.epoch,'lease_sequence',lease_row.lease_sequence,
      'envelope_digest',lease_row.envelope_digest,
      'lease_chain_digest',lease_row.lease_chain_digest);
  END IF;
  SELECT * INTO prior_row
    FROM okg.external_action_execution_epoch_leases
   WHERE deployment_name=bound_deployment AND epoch=(p_command->>'epoch')::bigint
     AND lease_sequence=(p_command->>'lease_sequence')::bigint
   FOR UPDATE;
  IF prior_row.lease_row_id IS NOT NULL THEN
    RAISE EXCEPTION 'external action lease sequence was reused with a different envelope'
      USING ERRCODE='40001';
  END IF;
  IF (p_command->>'lease_sequence')::bigint = 1 THEN
    IF existing_lease_count = 0 THEN
      IF p_command->>'previous_lease_digest' IS NOT NULL
         OR p_command->>'previous_epoch_digest' IS NOT NULL THEN
        RAISE EXCEPTION 'first external action lease has a predecessor'
          USING ERRCODE='40001';
      END IF;
    ELSE
      SELECT * INTO prior_epoch_row
        FROM okg.external_action_execution_epoch_leases
       WHERE deployment_name=bound_deployment AND epoch < (p_command->>'epoch')::bigint
       ORDER BY epoch DESC, lease_sequence DESC LIMIT 1 FOR UPDATE;
      IF prior_epoch_row.lease_row_id IS NULL
         OR p_command->>'previous_lease_digest' IS NOT NULL
         OR p_command->>'previous_epoch_digest' <> prior_epoch_row.envelope_digest THEN
        RAISE EXCEPTION 'external action lease epoch predecessor is invalid'
          USING ERRCODE='40001';
      END IF;
    END IF;
  ELSE
    SELECT * INTO prior_row
      FROM okg.external_action_execution_epoch_leases
     WHERE deployment_name=bound_deployment AND epoch=(p_command->>'epoch')::bigint
       AND lease_sequence=(p_command->>'lease_sequence')::bigint-1
     FOR UPDATE;
    IF prior_row.lease_row_id IS NULL
       OR p_command->>'previous_lease_digest' <> prior_row.envelope_digest
       OR p_command->>'previous_epoch_digest' IS DISTINCT FROM prior_row.previous_epoch_digest THEN
      RAISE EXCEPTION 'external action lease chain predecessor is invalid'
        USING ERRCODE='40001';
    END IF;
  END IF;
  INSERT INTO okg.external_action_execution_epoch_leases (
    deployment_name,epoch,lease_sequence,previous_lease_digest,previous_epoch_digest,
    issued_at,expires_at,signer_separation_subject,nonce,protected_b64,payload_b64,
    signature_b64,signer_key_id_digest,envelope_digest,verified_at,verification_source,
    verification_config_digest,verification_login_binding_digest,lease_chain_digest,
    authentication_audit_id,nomos_decision_id,store_record
  ) VALUES (
    bound_deployment,(p_command->>'epoch')::bigint,
    (p_command->>'lease_sequence')::bigint,p_command->>'previous_lease_digest',
    p_command->>'previous_epoch_digest',(p_command->>'issued_at')::timestamptz,
    (p_command->>'expires_at')::timestamptz,p_command->>'signer_separation_subject',
    p_command->>'nonce',p_command->>'protected_b64',p_command->>'payload_b64',
    p_command->>'signature_b64',p_command->>'signer_key_id_digest',p_command->>'envelope_digest',
    clock_timestamp(),p_command->>'verification_source',
    p_command->>'verification_config_digest',p_command->>'verification_login_binding_digest',
    p_command->>'lease_chain_digest',p_command->>'authentication_audit_id',
    p_command->>'nomos_decision_id',p_command
  ) ON CONFLICT (deployment_name,envelope_digest) DO NOTHING;
  GET DIAGNOSTICS inserted_count=ROW_COUNT;
  SELECT * INTO lease_row
    FROM okg.external_action_execution_epoch_leases
   WHERE deployment_name=bound_deployment AND envelope_digest=p_command->>'envelope_digest'
   FOR UPDATE;
  IF lease_row.lease_row_id IS NULL THEN
    RAISE EXCEPTION 'external action lease was not stored' USING ERRCODE='40001';
  END IF;
  RETURN jsonb_build_object('replay',inserted_count<>1,'deployment',bound_deployment,
    'epoch',lease_row.epoch,'lease_sequence',lease_row.lease_sequence,
    'envelope_digest',lease_row.envelope_digest,
    'lease_chain_digest',lease_row.lease_chain_digest);
END
$external_action_store_insert_lease$;

-- Read paths call this fixed transition so expiry is decided by the database
-- clock and the resulting event is durable and at-most-once.
CREATE OR REPLACE FUNCTION okg.external_action_materialize_expiry(p_proposal_id TEXT)
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $external_action_materialize_expiry$
DECLARE
  bound_deployment TEXT := okg.external_action_session_deployment();
  proposal_row okg.external_action_proposals%ROWTYPE;
  prior_review_state TEXT;
  prior_execution_state TEXT;
  event_type_value TEXT;
  deadline_kind_value TEXT;
  deadline_value TIMESTAMPTZ;
  observed_at TIMESTAMPTZ := clock_timestamp();
  event_sequence BIGINT;
  previous_event_hash_value TEXT;
  event_hash_value TEXT;
  event_projection JSONB;
BEGIN
  PERFORM okg.external_action_verifier_capability_status();
  IF p_proposal_id IS NULL OR btrim(p_proposal_id)='' THEN
    RAISE EXCEPTION 'external action proposal id is required' USING ERRCODE='22023';
  END IF;
  PERFORM pg_advisory_xact_lock(
    hashtextextended('okg.external_action.deployment:'||bound_deployment,0)
  );
  SELECT * INTO proposal_row FROM okg.external_action_proposals
   WHERE deployment_name=bound_deployment AND proposal_id=p_proposal_id FOR UPDATE;
  IF proposal_row.proposal_id IS NULL THEN
    RAISE EXCEPTION 'external action proposal was not found' USING ERRCODE='P0002';
  END IF;
  IF proposal_row.review_state='expired' THEN
    RETURN jsonb_build_object('replay',TRUE,'expired',TRUE,'deployment',bound_deployment,
      'proposal_id',proposal_row.proposal_id,'proposal',okg.external_action_logical_json_record(to_jsonb(proposal_row))-'store_record');
  END IF;
  IF proposal_row.review_state IN ('pending','edited')
     AND observed_at >= proposal_row.effective_review_expires_at THEN
    prior_review_state:=proposal_row.review_state;
    prior_execution_state:=proposal_row.execution_state;
    event_type_value:='review_expired'; deadline_kind_value:='review_expires_at';
    deadline_value:=proposal_row.effective_review_expires_at;
  ELSIF proposal_row.review_state='approved'
        AND proposal_row.execution_state='ready'
        AND proposal_row.execute_before IS NOT NULL
        AND observed_at >= proposal_row.execute_before THEN
    prior_review_state:=proposal_row.review_state;
    prior_execution_state:=proposal_row.execution_state;
    event_type_value:='approval_expired'; deadline_kind_value:='execute_before';
    deadline_value:=proposal_row.execute_before;
  ELSE
    RETURN jsonb_build_object('replay',FALSE,'expired',FALSE,'deployment',bound_deployment,
      'proposal_id',proposal_row.proposal_id,'proposal',okg.external_action_logical_json_record(to_jsonb(proposal_row))-'store_record');
  END IF;
  UPDATE okg.external_action_proposals
     SET review_state='expired',execution_state='unavailable',
         lifecycle_version=lifecycle_version+1,updated_at=observed_at,expired_at=observed_at,
         store_record=COALESCE(store_record,'{}'::jsonb)||jsonb_build_object(
           'review_state','expired','execution_state','unavailable',
           'lifecycle_version',lifecycle_version+1,'updated_at',observed_at,
           'expired_at',observed_at)
   WHERE deployment_name=bound_deployment AND proposal_id=proposal_row.proposal_id
  RETURNING * INTO proposal_row;
  SELECT COALESCE(max(sequence),0)+1,
         (array_agg(event_hash ORDER BY sequence))[array_length(array_agg(event_hash ORDER BY sequence),1)]
    INTO event_sequence,previous_event_hash_value
    FROM okg.external_action_events
   WHERE deployment_name=bound_deployment AND effect_instance_key=proposal_row.effect_instance_key;
  event_projection := jsonb_build_object(
    'proposal_id', proposal_row.proposal_id,
    'execution_epoch', proposal_row.execution_epoch,
    'effect_instance_key', proposal_row.effect_instance_key,
    'sequence', event_sequence,
    'lifecycle_version', proposal_row.lifecycle_version,
    'event_type', event_type_value,
    'actor_kind', 'system_clock',
    'actor_principal', NULL::TEXT,
    'actor_separation_subject', NULL::TEXT,
    'authentication_audit_ref', NULL::TEXT,
    'nomos_decision_ref', NULL::TEXT,
    'deadline_kind', deadline_kind_value,
    'policy_digest', NULL::TEXT,
    'before_review_state', prior_review_state,
    'after_review_state', 'expired',
    'before_execution_state', prior_execution_state,
    'after_execution_state', 'unavailable',
    'payload_version', proposal_row.payload_version,
    'payload_hash', proposal_row.current_payload_hash,
    'decision_id', proposal_row.current_decision_id,
    'decision_binding_hash', proposal_row.current_decision_binding_hash,
    'execution_id', NULL::TEXT,
    'execution_binding_hash', NULL::TEXT,
    'transition_reason_code', NULL::TEXT,
    'outcome', NULL::TEXT,
    'outcome_reason_code', NULL::TEXT,
    'outcome_request_hash', NULL::TEXT,
    'restore_recovery_digest', NULL::TEXT,
    'effective_at', to_char(deadline_value AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
    'recorded_at', to_char(observed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
    'previous_event_hash', previous_event_hash_value
  );
  event_hash_value:=okg.external_action_hash('okg-external-action-event-v1',event_projection);
  INSERT INTO okg.external_action_events (
    deployment_name,proposal_id,execution_epoch,effect_instance_key,sequence,lifecycle_version,
    event_type,actor_kind,actor_principal,actor_separation_subject,
    authentication_audit_ref,nomos_decision_ref,deadline_kind,policy_digest,
    before_review_state,after_review_state,before_execution_state,after_execution_state,
    payload_version,payload_hash,decision_id,decision_binding_hash,execution_id,
    execution_binding_hash,transition_reason_code,outcome,outcome_reason_code,
    outcome_request_hash,restore_recovery_digest,effective_at,recorded_at,
    previous_event_hash,event_hash,store_record
  ) VALUES (
    bound_deployment,proposal_row.proposal_id,proposal_row.execution_epoch,
    proposal_row.effect_instance_key,event_sequence,proposal_row.lifecycle_version,event_type_value,
    'system_clock',NULL,NULL,NULL,NULL,deadline_kind_value,NULL,prior_review_state,'expired',
    prior_execution_state,'unavailable',proposal_row.payload_version,
    proposal_row.current_payload_hash,proposal_row.current_decision_id,
    proposal_row.current_decision_binding_hash,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
    deadline_value,observed_at,previous_event_hash_value,event_hash_value,
    jsonb_build_object('event_projection', event_projection)
  ) ON CONFLICT (deployment_name,effect_instance_key,event_hash) DO NOTHING;
  RETURN jsonb_build_object('replay',FALSE,'expired',TRUE,'deployment',bound_deployment,
    'proposal_id',proposal_row.proposal_id,'event_hash',event_hash_value,
    'proposal',okg.external_action_logical_json_record(to_jsonb(proposal_row))-'store_record');
END
$external_action_materialize_expiry$;

DO $external_action_semantic_function_grants$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname='okg_external_action_api') THEN
    GRANT EXECUTE ON FUNCTION okg.external_action_session_deployment() TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_assert_deployment(TEXT) TO okg_external_action_api;
    REVOKE ALL ON FUNCTION okg.external_action_startup_login_binding() FROM PUBLIC;
    REVOKE ALL ON FUNCTION okg.external_action_record_auth_audit(JSONB) FROM PUBLIC;
    REVOKE ALL ON FUNCTION okg.external_action_nomos_authority_snapshot(TEXT,TEXT,TEXT,TEXT,TEXT) FROM PUBLIC;
    REVOKE ALL ON FUNCTION okg.external_action_nomos_persist_decision(JSONB) FROM PUBLIC;
    REVOKE ALL ON FUNCTION okg.external_action_submit(JSONB) FROM PUBLIC;
    REVOKE ALL ON FUNCTION okg.external_action_review_transition(JSONB) FROM PUBLIC;
    REVOKE ALL ON FUNCTION okg.external_action_claim(JSONB) FROM PUBLIC;
    REVOKE ALL ON FUNCTION okg.external_action_prepare_claim(JSONB) FROM PUBLIC;
    REVOKE ALL ON FUNCTION okg.external_action_authorized_read(JSONB) FROM PUBLIC;
    REVOKE ALL ON FUNCTION okg.external_action_report_outcome(JSONB) FROM PUBLIC;
    REVOKE ALL ON FUNCTION okg.external_action_owner_unavailability(JSONB) FROM PUBLIC;
    REVOKE ALL ON FUNCTION okg.external_action_restore_create(JSONB) FROM PUBLIC;
    REVOKE ALL ON FUNCTION okg.external_action_restore_batch(JSONB) FROM PUBLIC;
    REVOKE ALL ON FUNCTION okg.external_action_restore_finalize(JSONB) FROM PUBLIC;
    REVOKE ALL ON FUNCTION okg.external_action_store_insert_lease(JSONB) FROM PUBLIC;
    REVOKE ALL ON FUNCTION okg.external_action_materialize_expiry(TEXT) FROM PUBLIC;
    GRANT EXECUTE ON FUNCTION okg.external_action_record_auth_audit(JSONB) TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_nomos_authority_snapshot(TEXT,TEXT,TEXT,TEXT,TEXT) TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_verifier_capability_status() TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_nomos_persist_decision(JSONB) TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_submit(JSONB) TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_review_transition(JSONB) TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_claim(JSONB) TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_prepare_claim(JSONB) TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_authorized_read(JSONB) TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_report_outcome(JSONB) TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_owner_unavailability(JSONB) TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_create(JSONB) TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_batch(JSONB) TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_finalize(JSONB) TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_store_insert_lease(JSONB) TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_materialize_expiry(TEXT) TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_startup_login_binding() TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_claim_owner_unavailability(TEXT) TO okg_external_action_api;
  END IF;
  IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname='okg_external_action_function_owner') THEN
    GRANT SELECT ON okg.nomos_identity_providers, okg.nomos_subject_bindings TO okg_external_action_function_owner;
    GRANT SELECT, INSERT ON okg.nomos_decisions TO okg_external_action_function_owner;
    GRANT USAGE, SELECT ON okg.nomos_decisions_decision_row_id_seq TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_session_deployment() TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_assert_deployment(TEXT) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_require_command(JSONB,TEXT[],TEXT[]) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_json_bytes(JSONB) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_logical_json_record(JSONB) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_hash(TEXT,JSONB) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_effect_key(JSONB) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_hmac_sha256(BYTEA,BYTEA) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_api_role_binding_guard() TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_verifier_capability_status() TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_verify_verifier_assertion(JSONB,TEXT,JSONB) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_startup_login_binding() TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_claim_owner_unavailability(TEXT) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_nomos_authority_snapshot(TEXT,TEXT,TEXT,TEXT,TEXT) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_nomos_policy_bundle(TEXT,TEXT,TEXT) TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_require_command(JSONB,TEXT[],TEXT[]) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_json_bytes(JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_logical_json_record(JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_hash(TEXT,JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_effect_key(JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_startup_login_binding() OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_claim_owner_unavailability(TEXT) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_prepare_claim(JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_authorized_read(JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_record_auth_audit(JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_hmac_sha256(BYTEA,BYTEA) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_api_role_binding_guard() OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_verifier_capability_status() OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_verify_verifier_assertion(JSONB,TEXT,JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_nomos_authority_snapshot(TEXT,TEXT,TEXT,TEXT,TEXT) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_nomos_persist_decision(JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_submit(JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_review_transition(JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_claim(JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_report_outcome(JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_owner_unavailability(JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_restore_create(JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_restore_batch(JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_restore_finalize(JSONB) OWNER TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_store_insert_lease(JSONB) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_materialize_expiry(TEXT) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_hash(TEXT,JSONB) TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_hash(TEXT,JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_store_insert_lease(JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_materialize_expiry(TEXT) OWNER TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_nomos_policy_bundle(TEXT,TEXT,TEXT) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_store_insert_lease(JSONB) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_materialize_expiry(TEXT) TO okg_external_action_function_owner;
  END IF;
END
$external_action_semantic_function_grants$;

-- Final restore command implementations are defined after all historical
-- definitions so an upgrade cannot leave the permissive legacy body active.
CREATE OR REPLACE FUNCTION okg.external_action_restore_batch(p_command JSONB)
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,okg
AS $external_action_restore_batch_hardened$
DECLARE bound_deployment TEXT:=okg.external_action_session_deployment();
  session_row okg.external_action_restore_sessions%ROWTYPE;
  header_row okg.external_action_restore_checkpoint_staging%ROWTYPE;
  existing_header okg.external_action_restore_checkpoint_staging%ROWTYPE;
  entry JSONB; entries JSONB; key_value TEXT; previous_key TEXT; ordinal_value BIGINT:=0;
  item_count BIGINT; distinct_count BIGINT; header_checkpoint JSONB; header_entries JSONB;
  request_projection JSONB; expected_request_hash TEXT;
  observed_at TIMESTAMPTZ:=clock_timestamp();
BEGIN
  PERFORM okg.external_action_verifier_capability_status();
  PERFORM okg.external_action_require_command(
    p_command,
    ARRAY['deployment','restore_id','batch_index','request_hash','idempotency_key_hash',
      'operator_separation_subject','restore_attestation_envelope_digest','checkpoints',
      'checkpoint_envelope_digest','proof_digest','checkpoint_payload',
      'authentication_audit_id','nomos_decision_id'],
    ARRAY['deployment','restore_id','batch_index','request_hash','idempotency_key_hash',
      'operator_separation_subject','restore_attestation_envelope_digest','checkpoints',
      'authentication_audit_id','nomos_decision_id']
  );
  IF p_command->>'deployment' IS DISTINCT FROM bound_deployment THEN
    RAISE EXCEPTION 'external action deployment is not bound to session_user' USING ERRCODE='28000';
  END IF;
  IF (p_command->>'batch_index')::bigint<0 OR p_command->>'batch_index' !~ '^(0|[1-9][0-9]*)$' THEN
    RAISE EXCEPTION 'restore batch index is not canonical' USING ERRCODE='22023';
  END IF;
  entries:=p_command->'checkpoints';
  IF jsonb_typeof(entries) IS DISTINCT FROM 'array' OR jsonb_array_length(entries)<1
     OR jsonb_array_length(entries)>256 THEN
    RAISE EXCEPTION 'restore checkpoint batch must contain 1..256 entries' USING ERRCODE='22023';
  END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended('okg.external_action.deployment:'||bound_deployment,0));
  PERFORM okg.external_action_nomos_authority_snapshot(bound_deployment,p_command->>'authentication_audit_id',p_command->>'nomos_decision_id','external_action.restore_reconcile',p_command->>'request_hash');
  SELECT * INTO session_row FROM okg.external_action_restore_sessions
   WHERE deployment_name=bound_deployment AND restore_id=p_command->>'restore_id' FOR UPDATE;
  IF session_row.restore_session_row_id IS NULL OR session_row.status NOT IN ('staging','quarantined') THEN
    RAISE EXCEPTION 'restore session is not accepting checkpoint batches' USING ERRCODE='40001';
  END IF;
  IF session_row.operator_separation_subject IS DISTINCT FROM p_command->>'operator_separation_subject'
     OR session_row.restore_attestation_envelope_digest IS DISTINCT FROM p_command->>'restore_attestation_envelope_digest' THEN
    RAISE EXCEPTION 'restore batch operator or attestation is not bound to the session' USING ERRCODE='42501';
  END IF;
  IF EXISTS (SELECT 1 FROM okg.external_action_restore_completion_receipts
    WHERE deployment_name=bound_deployment AND restore_id=session_row.restore_id) THEN
    RAISE EXCEPTION 'restore session is already complete'
      USING ERRCODE='40001',
        DETAIL=jsonb_build_object(
          'classification','database_restore_terminal_conflict',
          'quarantine_state','clear'
        )::text;
  END IF;
  -- An exact retry is resolved from its immutable header before any effect
  -- key duplicate guard.  The retained request hash is the cryptographic
  -- identity of the already verified batch.
  SELECT * INTO existing_header FROM okg.external_action_restore_checkpoint_staging
   WHERE deployment_name=bound_deployment AND restore_id=session_row.restore_id
     AND batch_index=(p_command->>'batch_index')::bigint FOR UPDATE;
  IF existing_header.checkpoint_staging_row_id IS NOT NULL THEN
    -- Resolve both exact replay and a key/request collision before looking at
    -- effect keys in the new body.  Otherwise a retry with a changed key can
    -- leak the duplicate-effect diagnostic instead of the batch conflict.
    IF existing_header.operator_separation_subject IS DISTINCT FROM p_command->>'operator_separation_subject'
       OR existing_header.idempotency_key_hash IS DISTINCT FROM p_command->>'idempotency_key_hash'
       OR existing_header.request_hash IS DISTINCT FROM p_command->>'request_hash' THEN
      RAISE EXCEPTION 'restore batch replay conflicts with the first operator, key, or request' USING ERRCODE='40001';
    END IF;
    RETURN jsonb_build_object('replay',TRUE,'deployment',bound_deployment,'restore_id',session_row.restore_id,
      'batch_index',existing_header.batch_index,'accepted_checkpoint_count',
      (SELECT count(*) FROM okg.external_action_restore_checkpoint_entries e WHERE e.deployment_name=bound_deployment AND e.restore_id=session_row.restore_id AND e.batch_index=existing_header.batch_index),
      'quarantine_state','blocked');
  END IF;
  FOR entry IN SELECT value FROM jsonb_array_elements(entries) LOOP
    IF jsonb_typeof(entry) IS DISTINCT FROM 'object' THEN
      RAISE EXCEPTION 'restore checkpoint entry must be an object' USING ERRCODE='22023';
    END IF;
    IF EXISTS (SELECT 1 FROM jsonb_object_keys(entry) k
      WHERE k NOT IN ('effect_instance_key','checkpoint_envelope_digest','proof_digest','checkpoint_payload','checkpoint_envelope','proof'))
       OR NOT (entry ? 'effect_instance_key') OR NOT (entry ? 'checkpoint_envelope_digest')
       OR NOT (entry ? 'proof_digest') OR NOT (entry ? 'checkpoint_payload')
       OR NOT (entry ? 'checkpoint_envelope') OR NOT (entry ? 'proof') THEN
      RAISE EXCEPTION 'restore checkpoint entry has missing or extra fields' USING ERRCODE='22023';
    END IF;
    IF jsonb_typeof(entry->'checkpoint_payload') IS DISTINCT FROM 'object'
       OR jsonb_typeof(entry->'checkpoint_envelope') IS DISTINCT FROM 'object'
       OR jsonb_typeof(entry->'proof') IS DISTINCT FROM 'object' THEN
      RAISE EXCEPTION 'restore checkpoint evidence must be objects' USING ERRCODE='22023';
    END IF;
    IF EXISTS (
      SELECT 1 FROM jsonb_object_keys(entry->'checkpoint_payload') k
       WHERE k NOT IN (
         'deployment','effect_instance_key','execution_epoch',
         'ledger_fence_envelope_digest','fenced_ledger_root_digest',
         'fenced_ledger_entry_count','proof_kind','positive_proof_digest',
         'nonmembership_proof_digest','ledger_entry_digest',
         'submission_content_hash','proposal_id','proposal_request_hash',
         'original_payload_hash','approved_payload_hash','action_contract_digest',
         'decision_id','decision_binding_hash','execution_id',
         'execution_binding_hash','okg_claim_handoff_receipt_digest',
         'provider_idempotency_key_digest','journal_phase','effect_started_at',
         'provider_receipt_digest','outcome','outcome_reason_code',
         'outcome_recorded_at','journal_digest','pre_restore_effect_event_head_hash',
         'checkpoint_issued_at'))
      OR EXISTS (
        SELECT 1 FROM unnest(ARRAY[
          'deployment','effect_instance_key','execution_epoch',
          'ledger_fence_envelope_digest','fenced_ledger_root_digest',
          'fenced_ledger_entry_count','proof_kind','positive_proof_digest',
          'nonmembership_proof_digest','ledger_entry_digest',
          'submission_content_hash','proposal_id','proposal_request_hash',
          'original_payload_hash','approved_payload_hash','action_contract_digest',
          'decision_id','decision_binding_hash','execution_id',
          'execution_binding_hash','okg_claim_handoff_receipt_digest',
          'provider_idempotency_key_digest','journal_phase','effect_started_at',
          'provider_receipt_digest','outcome','outcome_reason_code',
          'outcome_recorded_at','journal_digest','pre_restore_effect_event_head_hash',
          'checkpoint_issued_at']::text[]) field
         WHERE NOT (entry->'checkpoint_payload' ? field)) THEN
      RAISE EXCEPTION 'restore checkpoint payload is not the closed signed projection' USING ERRCODE='22023';
    END IF;
    key_value:=entry->>'effect_instance_key';
    IF key_value IS NULL OR btrim(key_value)='' OR (entry->>'checkpoint_envelope_digest') !~ '^sha256:[0-9a-f]{64}$'
       OR (entry->'checkpoint_envelope'->>'envelope_digest') IS DISTINCT FROM entry->>'checkpoint_envelope_digest'
       OR (entry->>'proof_digest') !~ '^sha256:[0-9a-f]{64}$' THEN
      RAISE EXCEPTION 'restore checkpoint digest or effect key is invalid' USING ERRCODE='22023';
    END IF;
    IF previous_key IS NOT NULL AND convert_to(key_value,'UTF8')<=convert_to(previous_key,'UTF8') THEN
      RAISE EXCEPTION 'restore checkpoint effect keys must be sorted and unique' USING ERRCODE='22023';
    END IF;
    IF entry->'checkpoint_payload'->>'proof_kind'='positive'
       AND (entry->'proof'->>'leaf_index' IS NULL
            OR entry->'proof'->>'leaf_index' !~ '^(0|[1-9][0-9]*)$') THEN
      RAISE EXCEPTION 'positive restore proof leaf index is not canonical' USING ERRCODE='22023';
    END IF;
    previous_key:=key_value; ordinal_value:=ordinal_value+1;
  END LOOP;
  SELECT count(*),count(DISTINCT e->>'effect_instance_key') INTO item_count,distinct_count FROM jsonb_array_elements(entries) e;
  IF item_count<>distinct_count OR EXISTS (SELECT 1 FROM jsonb_array_elements(entries) e
    JOIN okg.external_action_restore_checkpoint_entries old ON old.deployment_name=bound_deployment AND old.restore_id=session_row.restore_id AND old.effect_instance_key=e->>'effect_instance_key') THEN
    RAISE EXCEPTION 'restore checkpoint effect key is duplicated' USING ERRCODE='40001';
  END IF;
  SELECT COALESCE(jsonb_agg(jsonb_build_object(
      'effect_instance_key',e->>'effect_instance_key',
      'checkpoint_envelope_digest',e->>'checkpoint_envelope_digest',
      'proof_digest',e->>'proof_digest','checkpoint_payload',e->'checkpoint_payload'-'classification')
      ORDER BY x.ordinality),'[]'::jsonb),
         jsonb_build_object(
           'restore_id',session_row.restore_id,
           'batch_index',(p_command->>'batch_index')::bigint,
           'checkpoints',COALESCE(jsonb_agg(jsonb_build_object(
             'effect_instance_key',e->>'effect_instance_key',
             'checkpoint_envelope_digest',e->>'checkpoint_envelope_digest',
             'proof_digest',e->>'proof_digest') ORDER BY x.ordinality),'[]'::jsonb))
    INTO header_entries,request_projection
    FROM jsonb_array_elements(entries) WITH ORDINALITY AS x(e,ordinality);
  expected_request_hash:=okg.external_action_hash(
    'okg-external-action-restore-batch-request-v1',request_projection);
  IF p_command->>'request_hash' IS DISTINCT FROM expected_request_hash THEN
    RAISE EXCEPTION 'restore batch request hash is not canonical' USING ERRCODE='40001';
  END IF;
  SELECT * INTO existing_header FROM okg.external_action_restore_checkpoint_staging
   WHERE deployment_name=bound_deployment AND restore_id=session_row.restore_id
     AND batch_index=(p_command->>'batch_index')::bigint FOR UPDATE;
  IF existing_header.checkpoint_staging_row_id IS NOT NULL THEN
    IF existing_header.operator_separation_subject IS DISTINCT FROM p_command->>'operator_separation_subject'
       OR existing_header.idempotency_key_hash IS DISTINCT FROM p_command->>'idempotency_key_hash'
       OR existing_header.request_hash IS DISTINCT FROM p_command->>'request_hash'
       OR existing_header.checkpoint_payload IS DISTINCT FROM header_entries THEN
      RAISE EXCEPTION 'restore batch replay conflicts with the first operator, key, or request' USING ERRCODE='40001';
    END IF;
    RETURN jsonb_build_object('replay',TRUE,'deployment',bound_deployment,'restore_id',session_row.restore_id,
      'batch_index',existing_header.batch_index,'accepted_checkpoint_count',
      (SELECT count(*) FROM okg.external_action_restore_checkpoint_entries e WHERE e.deployment_name=bound_deployment AND e.restore_id=session_row.restore_id AND e.batch_index=existing_header.batch_index),
      'quarantine_state','blocked');
  END IF;
  SELECT COALESCE(jsonb_agg(to_jsonb(e->>'checkpoint_envelope_digest') ORDER BY convert_to(e->>'checkpoint_envelope_digest','UTF8')),'[]'::jsonb)
    INTO header_checkpoint FROM jsonb_array_elements(entries) e;
  INSERT INTO okg.external_action_restore_checkpoint_staging
    (deployment_name,restore_id,batch_index,request_hash,checkpoint_envelope_digest,proof_digest,checkpoint_payload,staged_at,store_record,effect_instance_key,operator_separation_subject,idempotency_key_hash)
  VALUES (bound_deployment,session_row.restore_id,(p_command->>'batch_index')::bigint,p_command->>'request_hash',
    okg.external_action_hash('okg-external-action-restore-checkpoint-set-v1',jsonb_build_object('sorted_checkpoint_envelope_digests',header_checkpoint,'count',item_count)),
    okg.external_action_hash('okg-external-action-restore-proof-batch-v1',entries),header_entries,observed_at,p_command,NULL,
    p_command->>'operator_separation_subject',p_command->>'idempotency_key_hash') RETURNING * INTO header_row;
  ordinal_value:=0;
  FOR entry IN SELECT value FROM jsonb_array_elements(entries) LOOP
    ordinal_value:=ordinal_value+1;
    INSERT INTO okg.external_action_restore_checkpoint_entries
      (deployment_name,restore_id,batch_index,ordinal,effect_instance_key,checkpoint_envelope_digest,proof_digest,leaf_index,proof,checkpoint_payload,staged_at)
    VALUES (bound_deployment,session_row.restore_id,header_row.batch_index,ordinal_value-1,entry->>'effect_instance_key',
      entry->>'checkpoint_envelope_digest',entry->>'proof_digest',NULLIF(entry->'proof'->>'leaf_index','')::bigint,entry->'proof',entry->'checkpoint_payload'-'classification',observed_at);
  END LOOP;
  RETURN jsonb_build_object('replay',FALSE,'deployment',bound_deployment,'restore_id',session_row.restore_id,
    'batch_index',header_row.batch_index,'accepted_checkpoint_count',item_count,'quarantine_state','blocked');
END
$external_action_restore_batch_hardened$;

-- Prepare returns the exact immutable inputs which the service must sign on
-- the same connection before calling finalize.  It performs no durable write
-- and therefore incomplete coverage never reserves an idempotency key.
CREATE OR REPLACE FUNCTION okg.external_action_restore_prepare(p_command JSONB)
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,okg
AS $external_action_restore_prepare$
DECLARE checked JSONB; root JSONB; attestation JSONB;
BEGIN
  checked:=okg.external_action_restore_assert_coverage(p_command);
  root:=okg.external_action_restore_event_root(checked->>'deployment');
  RETURN checked || jsonb_build_object(
    'restored_event_root_digest',root->>'digest',
    'restored_event_head_count',(root->>'count')::bigint,
    'completed_at',to_char(clock_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
  );
END
$external_action_restore_prepare$;

CREATE OR REPLACE FUNCTION okg.external_action_restore_finalize(p_command JSONB)
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,okg
AS $external_action_restore_finalize_hardened$
DECLARE bound_deployment TEXT:=okg.external_action_session_deployment();
  session_row okg.external_action_restore_sessions%ROWTYPE;
  attestation_row okg.external_action_restore_attestations%ROWTYPE;
  fence_row okg.external_action_restore_fences%ROWTYPE;
  existing_final okg.external_action_restore_finalize_idempotency%ROWTYPE;
  completion_row okg.external_action_restore_completion_receipts%ROWTYPE;
  checked JSONB; projection JSONB; unsigned JSONB; recovery_projection JSONB;
  recovery_set JSONB; pre_root JSONB; post_root JSONB; entry JSONB; payload JSONB;
  old_sequence BIGINT; previous_hash TEXT; event_hash_value TEXT; recovery_digest_value TEXT;
  effective_value TIMESTAMPTZ; recorded_value TIMESTAMPTZ; completed_value TIMESTAMPTZ;
  classification TEXT; source_proposal TEXT; source_decision TEXT; source_execution TEXT;
  response JSONB; coverage_command JSONB; operator_value TEXT; key_value TEXT; request_value TEXT;
  observed_at TIMESTAMPTZ:=clock_timestamp();
BEGIN
  PERFORM okg.external_action_require_command(
    p_command,
    ARRAY['deployment','restore_id','restore_attestation_envelope_digest','expected_inventory_digest',
      'expected_inventory_count','expected_checkpoint_set_digest','expected_checkpoint_count',
      'operator_separation_subject','idempotency_key_hash','request_hash','completion_receipt_envelope_digest',
      'completion_receipt_protected_b64','completion_receipt_payload_b64','completion_receipt_signature_b64',
      'completion_receipt_projection','completion_receipt_projection_digest','authentication_audit_id',
      'nomos_decision_id','service_mode','quarantine_state'],
    ARRAY['deployment','restore_id','restore_attestation_envelope_digest','expected_inventory_digest',
      'expected_inventory_count','expected_checkpoint_set_digest','expected_checkpoint_count',
      'completion_receipt_envelope_digest','completion_receipt_protected_b64','completion_receipt_payload_b64',
      'completion_receipt_signature_b64','completion_receipt_projection','completion_receipt_projection_digest',
      'authentication_audit_id','nomos_decision_id']
  );
  IF p_command->>'deployment' IS DISTINCT FROM bound_deployment THEN
    RAISE EXCEPTION 'external action deployment is not bound to session_user' USING ERRCODE='28000';
  END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended('okg.external_action.deployment:'||bound_deployment,0));
  SELECT * INTO session_row FROM okg.external_action_restore_sessions
   WHERE deployment_name=bound_deployment AND restore_id=p_command->>'restore_id' FOR UPDATE;
  IF session_row.restore_session_row_id IS NULL THEN
    RAISE EXCEPTION 'restore session is not finalizable' USING ERRCODE='40001';
  END IF;
  operator_value:=COALESCE(p_command->>'operator_separation_subject',session_row.operator_separation_subject);
  key_value:=COALESCE(p_command->>'idempotency_key_hash',session_row.idempotency_key_hash);
  request_value:=COALESCE(p_command->>'request_hash',session_row.request_hash);
  IF operator_value IS DISTINCT FROM session_row.operator_separation_subject THEN
    RAISE EXCEPTION 'restore finalize operator is not the session operator' USING ERRCODE='42501';
  END IF;
  SELECT * INTO existing_final FROM okg.external_action_restore_finalize_idempotency
   WHERE deployment_name=bound_deployment AND restore_id=session_row.restore_id FOR SHARE;
  IF existing_final.finalize_idempotency_row_id IS NOT NULL THEN
    IF existing_final.operator_separation_subject IS DISTINCT FROM operator_value
       OR existing_final.idempotency_key_hash IS DISTINCT FROM key_value
       OR existing_final.request_hash IS DISTINCT FROM request_value THEN
      RAISE EXCEPTION 'restore finalize replay conflicts with the first operator, key, or request' USING ERRCODE='40001';
    END IF;
    RETURN convert_from(existing_final.response_body_jcs,'UTF8')::jsonb;
  END IF;
  SELECT * INTO completion_row FROM okg.external_action_restore_completion_receipts
   WHERE deployment_name=bound_deployment AND restore_id=session_row.restore_id FOR SHARE;
  IF completion_row.completion_receipt_row_id IS NOT NULL THEN
    RAISE EXCEPTION 'restore completion exists without its finalize idempotency record' USING ERRCODE='40001';
  END IF;
  coverage_command:=jsonb_build_object('deployment',bound_deployment,'restore_id',p_command->>'restore_id',
    'restore_attestation_envelope_digest',p_command->>'restore_attestation_envelope_digest',
    'expected_inventory_digest',p_command->>'expected_inventory_digest','expected_inventory_count',p_command->>'expected_inventory_count',
    'expected_checkpoint_set_digest',p_command->>'expected_checkpoint_set_digest','expected_checkpoint_count',p_command->>'expected_checkpoint_count');
  checked:=okg.external_action_restore_assert_coverage(coverage_command);
  SELECT * INTO attestation_row FROM okg.external_action_restore_attestations
   WHERE deployment_name=bound_deployment AND envelope_digest=session_row.restore_attestation_envelope_digest FOR SHARE;
  pre_root:=okg.external_action_restore_event_root(bound_deployment);
  IF pre_root->>'digest' IS DISTINCT FROM attestation_row.pre_restore_event_root_digest
     OR (pre_root->>'count')::bigint IS DISTINCT FROM attestation_row.pre_restore_event_head_count THEN
    RAISE EXCEPTION 'restore event root is stale or does not match the attestation' USING ERRCODE='40001';
  END IF;
  projection:=p_command->'completion_receipt_projection';
  IF jsonb_typeof(projection) IS DISTINCT FROM 'object' OR
     projection->>'deployment' IS DISTINCT FROM bound_deployment OR
     projection->>'restore_id' IS DISTINCT FROM session_row.restore_id OR
     projection->>'restore_attestation_envelope_digest' IS DISTINCT FROM session_row.restore_attestation_envelope_digest OR
     projection->>'pre_restore_event_root_digest' IS DISTINCT FROM attestation_row.pre_restore_event_root_digest OR
     projection->>'pre_restore_event_head_count' IS DISTINCT FROM attestation_row.pre_restore_event_head_count THEN
    RAISE EXCEPTION 'completion receipt projection is not bound to the restore' USING ERRCODE='40001';
  END IF;
  IF okg.external_action_hash('okg-external-action-restore-completion-receipt-v1',projection)
     IS DISTINCT FROM p_command->>'completion_receipt_projection_digest' THEN
    RAISE EXCEPTION 'completion receipt projection digest is invalid' USING ERRCODE='22023';
  END IF;
  unsigned:=jsonb_build_object('protected_b64',p_command->>'completion_receipt_protected_b64',
    'payload_b64',p_command->>'completion_receipt_payload_b64','signature_b64',p_command->>'completion_receipt_signature_b64');
  IF p_command->>'completion_receipt_envelope_digest' IS DISTINCT FROM
     okg.external_action_hash('okg-external-action-restore-completion-receipt-envelope-v1',unsigned) THEN
    RAISE EXCEPTION 'completion receipt envelope digest is invalid' USING ERRCODE='22023';
  END IF;
  IF p_command->>'completion_receipt_protected_b64' !~ '^[A-Za-z0-9_-]+$'
     OR p_command->>'completion_receipt_payload_b64' !~ '^[A-Za-z0-9_-]+$'
     OR p_command->>'completion_receipt_signature_b64' !~ '^[A-Za-z0-9_-]+$' THEN
    RAISE EXCEPTION 'completion receipt envelope encoding is invalid' USING ERRCODE='22023';
  END IF;
  -- Each checkpoint carries the already verified signed worker projection.
  -- SQL validates its immutable relational binding and recovery classification;
  -- detached signature/proof verification remains in the Python authority.
  FOR entry IN SELECT e FROM okg.external_action_restore_checkpoint_entries e
    WHERE e.deployment_name=bound_deployment AND e.restore_id=session_row.restore_id
    ORDER BY convert_to(e.effect_instance_key,'UTF8') LOOP
    payload:=entry.e.checkpoint_payload;
    classification:=payload->>'classification';
    IF classification NOT IN ('database_restore_no_effect_started','database_restore_terminal_review_confirmed','database_restore_effect_uncertain','database_restore_succeeded','database_restore_failed','database_restore_terminal_confirmed','database_restore_terminal_conflict') THEN
      RAISE EXCEPTION 'restore checkpoint classification is invalid' USING ERRCODE='40001';
    END IF;
    effective_value:=COALESCE(NULLIF(payload->>'effective_at','')::timestamptz,observed_at);
    recorded_value:=observed_at;
    source_proposal:=NULLIF(payload->>'source_proposal_id','');
    source_decision:=NULLIF(payload->>'source_decision_id','');
    source_execution:=NULLIF(payload->>'source_execution_id','');
    recovery_projection:=jsonb_build_object('deployment',bound_deployment,'old_epoch',attestation_row.old_epoch,
      'new_epoch',attestation_row.new_epoch,'effect_instance_key',entry.e.effect_instance_key,
      'submission_content_hash',COALESCE(payload->>'submission_content_hash',''),'checkpoint_digest',entry.e.checkpoint_envelope_digest,
      'restore_attestation_digest',attestation_row.envelope_digest,'classification',classification,
      'source_proposal_id',source_proposal,'source_decision_id',source_decision,'source_execution_id',source_execution,
      'recovered_review_state',NULLIF(payload->>'recovered_review_state',''),'recovered_execution_state',NULLIF(payload->>'recovered_execution_state',''),
      'recovered_outcome',NULLIF(payload->>'recovered_outcome',''),'effective_at',to_char(effective_value AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
      'recorded_at',to_char(recorded_value AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),'pre_restore_effect_event_head_hash',NULLIF(payload->>'pre_restore_effect_event_head_hash',''));
    recovery_digest_value:=okg.external_action_hash('okg-external-action-restore-recovery-v1',recovery_projection);
    INSERT INTO okg.external_action_restore_recoveries
      (deployment_name,old_epoch,new_epoch,effect_instance_key,submission_content_hash,checkpoint_digest,restore_attestation_digest,classification,source_proposal_id,source_decision_id,source_execution_id,recovered_review_state,recovered_execution_state,recovered_outcome,effective_at,recorded_at,pre_restore_effect_event_head_hash,signed_worker_checkpoint_digest,recovery_digest,store_record)
    VALUES (bound_deployment,attestation_row.old_epoch,attestation_row.new_epoch,entry.e.effect_instance_key,
      COALESCE(payload->>'submission_content_hash',''),entry.e.checkpoint_envelope_digest,attestation_row.envelope_digest,classification,
      source_proposal,source_decision,source_execution,NULLIF(payload->>'recovered_review_state',''),NULLIF(payload->>'recovered_execution_state',''),NULLIF(payload->>'recovered_outcome',''),effective_value,recorded_value,NULLIF(payload->>'pre_restore_effect_event_head_hash',''),entry.e.checkpoint_envelope_digest,recovery_digest_value,payload);
    PERFORM pg_advisory_xact_lock(hashtextextended('okg.external_action.effect:'||entry.e.effect_instance_key,0));
    SELECT COALESCE(max(sequence),0), (array_agg(event_hash ORDER BY sequence))[array_length(array_agg(event_hash ORDER BY sequence),1)]
      INTO old_sequence,previous_hash FROM okg.external_action_events
     WHERE deployment_name=bound_deployment AND effect_instance_key=entry.e.effect_instance_key;
    event_hash_value:=okg.external_action_hash('okg-external-action-event-v1',jsonb_build_object(
      'deployment',bound_deployment,'proposal_id',source_proposal,'execution_epoch',attestation_row.new_epoch,
      'effect_instance_key',entry.e.effect_instance_key,'sequence',old_sequence+1,'lifecycle_version',1,
      'event_type','restore_recovery','actor_kind','restore_operator','actor_principal',operator_value,
      'actor_separation_subject',operator_value,'authentication_audit_ref',p_command->>'authentication_audit_id',
      'nomos_decision_ref',p_command->>'nomos_decision_id','restore_recovery_digest',recovery_digest_value,
      'effective_at',to_char(effective_value AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
      'recorded_at',to_char(recorded_value AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),'previous_event_hash',previous_hash));
    INSERT INTO okg.external_action_events
      (deployment_name,proposal_id,execution_epoch,effect_instance_key,sequence,lifecycle_version,event_type,actor_kind,actor_principal,actor_separation_subject,authentication_audit_ref,nomos_decision_ref,restore_recovery_digest,effective_at,recorded_at,previous_event_hash,event_hash,store_record)
    VALUES (bound_deployment,source_proposal,attestation_row.new_epoch,entry.e.effect_instance_key,old_sequence+1,1,'restore_recovered','restore_operator',operator_value,operator_value,p_command->>'authentication_audit_id',p_command->>'nomos_decision_id',recovery_digest_value,effective_value,recorded_value,previous_hash,event_hash_value,jsonb_build_object('checkpoint_digest',entry.e.checkpoint_envelope_digest));
  END LOOP;
  post_root:=okg.external_action_restore_event_root(bound_deployment);
  SELECT jsonb_build_object('sorted_restore_recovery_digests',COALESCE(jsonb_agg(to_jsonb(recovery_digest) ORDER BY convert_to(recovery_digest,'UTF8')),'[]'::jsonb),'count',count(*)) INTO recovery_set
    FROM okg.external_action_restore_recoveries WHERE deployment_name=bound_deployment AND restore_attestation_digest=attestation_row.envelope_digest;
  recovery_set:=recovery_set||jsonb_build_object('digest',okg.external_action_hash('okg-external-action-restore-recovery-set-v1',recovery_set));
  IF projection->>'restored_event_root_digest' IS DISTINCT FROM post_root->>'digest'
     OR (projection->>'restored_event_head_count')::bigint IS DISTINCT FROM (post_root->>'count')::bigint
     OR projection->>'recovery_set_digest' IS DISTINCT FROM recovery_set->>'digest'
     OR (projection->>'recovery_count')::bigint IS DISTINCT FROM (recovery_set->>'count')::bigint THEN
    RAISE EXCEPTION 'completion receipt projection does not match durable recovery result' USING ERRCODE='40001';
  END IF;
  completed_value:=(projection->>'completed_at')::timestamptz;
  IF completed_value IS NULL THEN RAISE EXCEPTION 'completion receipt has no completed_at' USING ERRCODE='22023'; END IF;
  response:=jsonb_build_object('replay',FALSE,'deployment',bound_deployment,'restore_id',session_row.restore_id,'state','complete','quarantine_state','cleared','service_mode','normal','recovery_count',(recovery_set->>'count')::bigint,'recovery_set_digest',recovery_set->>'digest','restore_completion_receipt',jsonb_build_object('envelope_digest',p_command->>'completion_receipt_envelope_digest'));
  INSERT INTO okg.external_action_restore_completion_receipts
    (deployment_name,restore_id,restore_attestation_envelope_digest,completion_receipt_envelope_digest,completed_at,authentication_audit_id,nomos_decision_id,store_record,protected_b64,payload_b64,signature_b64,completion_receipt_projection)
  VALUES (bound_deployment,session_row.restore_id,attestation_row.envelope_digest,p_command->>'completion_receipt_envelope_digest',completed_value,p_command->>'authentication_audit_id',p_command->>'nomos_decision_id',response,p_command->>'completion_receipt_protected_b64',p_command->>'completion_receipt_payload_b64',p_command->>'completion_receipt_signature_b64',projection);
  INSERT INTO okg.external_action_restore_finalize_idempotency
    (deployment_name,restore_id,operator_separation_subject,idempotency_key_hash,request_hash,response_body_jcs,completion_receipt_envelope_digest)
  VALUES (bound_deployment,session_row.restore_id,operator_value,key_value,request_value,okg.external_action_json_bytes(response),p_command->>'completion_receipt_envelope_digest');
  SELECT * INTO fence_row FROM okg.external_action_restore_fences WHERE deployment_name=bound_deployment FOR UPDATE;
  IF fence_row.deployment_name IS NULL OR fence_row.highest_accepted_epoch>attestation_row.new_epoch THEN
    RAISE EXCEPTION 'restore fence changed while finalizing' USING ERRCODE='40001';
  END IF;
  UPDATE okg.external_action_restore_fences
     SET highest_accepted_epoch=attestation_row.new_epoch,lease_digest=attestation_row.new_epoch_lease_digest,
         quarantine_state='clear',service_mode='normal',restore_attestation_digest=attestation_row.envelope_digest,
         accepted_at=completed_value,mode_changed_at=completed_value
   WHERE deployment_name=bound_deployment;
  RETURN response;
END
$external_action_restore_finalize_hardened$;

CREATE OR REPLACE FUNCTION okg.external_action_restore_lifecycle_gate()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,okg
AS $external_action_restore_lifecycle_gate$
DECLARE deployment_value TEXT; epoch_value BIGINT; fence_row okg.external_action_restore_fences%ROWTYPE;
BEGIN
  deployment_value:=NEW.deployment_name;
  epoch_value:=COALESCE(NEW.execution_epoch,0);
  -- Serialize every lifecycle insert with startup rotation and restore
  -- clearance.  A shared fence read alone permits an insert to slip between
  -- the read and a concurrent mode switch.
  PERFORM pg_advisory_xact_lock(
    hashtextextended('okg.external_action.deployment:'||deployment_value,0)
  );
  SELECT * INTO fence_row FROM okg.external_action_restore_fences
   WHERE deployment_name=deployment_value FOR SHARE;
  IF fence_row.deployment_name IS NOT NULL AND (fence_row.service_mode<>'normal' OR fence_row.quarantine_state<>'clear') THEN
    RAISE EXCEPTION 'external action lifecycle is quarantined for restore' USING ERRCODE='40001';
  END IF;
  IF fence_row.deployment_name IS NOT NULL AND epoch_value<fence_row.highest_accepted_epoch THEN
    RAISE EXCEPTION 'external action lifecycle epoch is stale' USING ERRCODE='40001';
  END IF;
  RETURN NEW;
END
$external_action_restore_lifecycle_gate$;
DROP TRIGGER IF EXISTS external_action_proposal_restore_gate ON okg.external_action_proposals;
CREATE TRIGGER external_action_proposal_restore_gate BEFORE INSERT ON okg.external_action_proposals
FOR EACH ROW EXECUTE FUNCTION okg.external_action_restore_lifecycle_gate();
DROP TRIGGER IF EXISTS external_action_decision_restore_gate ON okg.external_action_decisions;
CREATE TRIGGER external_action_decision_restore_gate BEFORE INSERT ON okg.external_action_decisions
FOR EACH ROW EXECUTE FUNCTION okg.external_action_restore_lifecycle_gate();
DROP TRIGGER IF EXISTS external_action_execution_restore_gate ON okg.external_action_executions;
CREATE TRIGGER external_action_execution_restore_gate BEFORE INSERT ON okg.external_action_executions
FOR EACH ROW EXECUTE FUNCTION okg.external_action_restore_lifecycle_gate();

-- Restore hardening.  The public API verifies the six signed envelopes and
-- passes their immutable projections to this database boundary.  SQL does
-- not pretend to verify detached signatures or Merkle proofs; it binds the
-- verified digests, inventory, and checkpoint projections transactionally and
-- refuses to clear quarantine unless all relational coverage checks pass.
CREATE OR REPLACE FUNCTION okg.external_action_restore_inventory_digest(p_inventory JSONB)
RETURNS TEXT
LANGUAGE plpgsql IMMUTABLE SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $external_action_restore_inventory_digest$
DECLARE sorted_inventory JSONB; item TEXT; previous_item TEXT;
BEGIN
  IF jsonb_typeof(p_inventory) IS DISTINCT FROM 'array' THEN
    RAISE EXCEPTION 'restore inventory must be an array' USING ERRCODE='22023';
  END IF;
  FOR item IN SELECT value FROM jsonb_array_elements_text(p_inventory) LOOP
    IF item IS NULL OR btrim(item)='' THEN
      RAISE EXCEPTION 'restore inventory contains an empty effect key' USING ERRCODE='22023';
    END IF;
    IF previous_item IS NOT NULL AND convert_to(item,'UTF8') <= convert_to(previous_item,'UTF8') THEN
      RAISE EXCEPTION 'restore inventory must be sorted and unique' USING ERRCODE='22023';
    END IF;
    previous_item := item;
  END LOOP;
  SELECT COALESCE(jsonb_agg(to_jsonb(value) ORDER BY convert_to(value #>> '{}','UTF8')),'[]'::jsonb)
    INTO sorted_inventory FROM jsonb_array_elements(p_inventory);
  IF sorted_inventory IS DISTINCT FROM p_inventory THEN
    RAISE EXCEPTION 'restore inventory must be canonically sorted' USING ERRCODE='22023';
  END IF;
  RETURN okg.external_action_hash(
    'okg-external-action-restore-inventory-v1',
    jsonb_build_object('sorted_effect_instance_keys',p_inventory,'count',jsonb_array_length(p_inventory))
  );
END
$external_action_restore_inventory_digest$;

-- Return the deployment-owned effect inventory used to bind a restore
-- attestation.  The restore id is an audit namespace only: the source of
-- truth is every persisted effect key in this deployment, including keys
-- whose current projection is terminal or partially recovered.  The caller
-- cannot provide or narrow this set.
CREATE OR REPLACE FUNCTION okg.external_action_restore_inventory(p_restore_id TEXT)
RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $external_action_restore_inventory$
DECLARE bound_deployment TEXT:=okg.external_action_session_deployment();
  inventory JSONB; inventory_count BIGINT; inventory_digest TEXT; old_epoch BIGINT;
BEGIN
  PERFORM okg.external_action_verifier_capability_status();
  IF p_restore_id IS NULL OR btrim(p_restore_id)='' THEN
    RAISE EXCEPTION 'restore inventory requires a restore id' USING ERRCODE='22023';
  END IF;
  SELECT highest_accepted_epoch INTO old_epoch
    FROM okg.external_action_restore_fences
   WHERE deployment_name=bound_deployment;
  IF old_epoch IS NULL THEN
    RAISE EXCEPTION 'restore inventory requires a persisted startup fence'
      USING ERRCODE='40001';
  END IF;
  -- After a rotation the fence points at the new epoch while the restore
  -- inventory belongs to the greatest persisted pre-rotation epoch.  On a
  -- renewal (or a first-use fence) that is the fence epoch itself.
  SELECT COALESCE(max(execution_epoch), old_epoch) INTO old_epoch
    FROM (
      SELECT execution_epoch FROM okg.external_action_proposals
       WHERE deployment_name=bound_deployment AND execution_epoch < old_epoch
      UNION ALL
      SELECT execution_epoch FROM okg.external_action_executions
       WHERE deployment_name=bound_deployment AND execution_epoch < old_epoch
      UNION ALL
      SELECT execution_epoch FROM okg.external_action_events
       WHERE deployment_name=bound_deployment AND execution_epoch < old_epoch
    ) epochs;
  SELECT COALESCE(jsonb_agg(to_jsonb(effect_instance_key)
      ORDER BY convert_to(effect_instance_key,'UTF8')),'[]'::jsonb)
    INTO inventory
    FROM (
      SELECT effect_instance_key FROM okg.external_action_proposals
       WHERE deployment_name=bound_deployment AND execution_epoch=old_epoch
      UNION
      SELECT d.effect_instance_key FROM okg.external_action_decisions d
       JOIN okg.external_action_proposals p ON p.deployment_name=d.deployment_name AND p.proposal_id=d.proposal_id
       WHERE d.deployment_name=bound_deployment AND p.execution_epoch=old_epoch
      UNION
      SELECT effect_instance_key FROM okg.external_action_executions
       WHERE deployment_name=bound_deployment AND execution_epoch=old_epoch
      UNION
      SELECT o.effect_instance_key FROM okg.external_action_outcomes o
       JOIN okg.external_action_executions e ON e.deployment_name=o.deployment_name AND e.execution_id=o.execution_id
       WHERE o.deployment_name=bound_deployment AND e.execution_epoch=old_epoch
      UNION
      SELECT effect_instance_key FROM okg.external_action_events
       WHERE deployment_name=bound_deployment AND execution_epoch=old_epoch
    ) owned_keys;
  inventory_count:=jsonb_array_length(inventory);
  inventory_digest:=okg.external_action_restore_inventory_digest(inventory);
  RETURN jsonb_build_object(
    'deployment',bound_deployment,'restore_id',p_restore_id,
    'sorted_effect_instance_keys',inventory,'count',inventory_count,
    'digest',inventory_digest);
END
$external_action_restore_inventory$;

CREATE OR REPLACE FUNCTION okg.external_action_restore_checkpoint_set_digest(p_restore_id TEXT)
RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $external_action_restore_checkpoint_set_digest$
DECLARE digests JSONB; count_value BIGINT;
BEGIN
  SELECT count(*)::bigint,
         COALESCE(jsonb_agg(to_jsonb(checkpoint_envelope_digest)
           ORDER BY convert_to(checkpoint_envelope_digest,'UTF8')),'[]'::jsonb)
    INTO count_value, digests
    FROM okg.external_action_restore_checkpoint_entries
   WHERE deployment_name=okg.external_action_session_deployment() AND restore_id=p_restore_id;
  RETURN jsonb_build_object(
    'sorted_checkpoint_envelope_digests',digests,
    'count',count_value,
    'digest',okg.external_action_hash(
      'okg-external-action-restore-checkpoint-set-v1',
      jsonb_build_object('sorted_checkpoint_envelope_digests',digests,'count',count_value)
    )
  );
END
$external_action_restore_checkpoint_set_digest$;

CREATE OR REPLACE FUNCTION okg.external_action_restore_event_root(p_deployment TEXT)
RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $external_action_restore_event_root$
DECLARE heads JSONB; count_value BIGINT;
BEGIN
  SELECT count(*)::bigint,
         COALESCE(jsonb_agg(jsonb_build_object(
           'effect_instance_key',effect_instance_key,'sequence',sequence,'event_hash',event_hash
         ) ORDER BY convert_to(effect_instance_key,'UTF8')),'[]'::jsonb)
    INTO count_value,heads
    FROM (
      SELECT DISTINCT ON (effect_instance_key) effect_instance_key,sequence,event_hash
        FROM okg.external_action_events
       WHERE deployment_name=p_deployment
       ORDER BY effect_instance_key,sequence DESC
    ) latest;
  RETURN jsonb_build_object(
    'sorted_event_head_entries',heads,'count',count_value,
    'digest',okg.external_action_hash(
      'okg-external-action-restore-event-root-v1',
      jsonb_build_object('sorted_event_head_entries',heads,'count',count_value)
    )
  );
END
$external_action_restore_event_root$;

CREATE OR REPLACE FUNCTION okg.external_action_restore_assert_coverage(p_command JSONB)
RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $external_action_restore_assert_coverage$
DECLARE bound_deployment TEXT:=okg.external_action_session_deployment();
  session_row okg.external_action_restore_sessions%ROWTYPE;
  attestation_row okg.external_action_restore_attestations%ROWTYPE;
  fence_row okg.external_action_restore_fences%ROWTYPE;
  keys JSONB; inventory JSONB; db_inventory JSONB; derived_inventory JSONB; checkpoint_set JSONB; count_value BIGINT;
  inventory_count BIGINT; derived_inventory_count BIGINT; derived_inventory_digest TEXT;
  expected_inventory_digest TEXT; expected_checkpoint_digest TEXT; pre_root JSONB;
  positive_count BIGINT; positive_distinct_count BIGINT; positive_min BIGINT; positive_max BIGINT; positive_previous TEXT; last_positive_key TEXT;
BEGIN
  PERFORM okg.external_action_require_command(
    p_command,
    ARRAY['deployment','restore_id','restore_attestation_envelope_digest',
      'expected_inventory_digest','expected_inventory_count',
      'expected_checkpoint_set_digest','expected_checkpoint_count'],
    ARRAY['deployment','restore_id','restore_attestation_envelope_digest',
      'expected_inventory_digest','expected_inventory_count',
      'expected_checkpoint_set_digest','expected_checkpoint_count']
  );
  IF p_command->>'deployment' IS DISTINCT FROM bound_deployment THEN
    RAISE EXCEPTION 'external action deployment is not bound to session_user' USING ERRCODE='28000';
  END IF;
  SELECT * INTO session_row FROM okg.external_action_restore_sessions
   WHERE deployment_name=bound_deployment AND restore_id=p_command->>'restore_id' FOR UPDATE;
  IF session_row.restore_session_row_id IS NULL THEN
    RAISE EXCEPTION 'restore session is not finalizable' USING ERRCODE='40001';
  END IF;
  SELECT * INTO fence_row FROM okg.external_action_restore_fences
   WHERE deployment_name=bound_deployment FOR UPDATE;
  IF fence_row.deployment_name IS NULL OR fence_row.service_mode<>'restore_only'
     OR fence_row.quarantine_state<>'blocked' THEN
    RAISE EXCEPTION 'restore fence is not blocked in restore_only mode' USING ERRCODE='40001';
  END IF;
  IF session_row.restore_attestation_envelope_digest IS DISTINCT FROM
     p_command->>'restore_attestation_envelope_digest' THEN
    RAISE EXCEPTION 'restore attestation does not match session' USING ERRCODE='40001';
  END IF;
  SELECT * INTO attestation_row FROM okg.external_action_restore_attestations
   WHERE deployment_name=bound_deployment AND envelope_digest=session_row.restore_attestation_envelope_digest
   FOR SHARE;
  IF attestation_row.attestation_row_id IS NULL THEN
    RAISE EXCEPTION 'restore attestation projection is unavailable' USING ERRCODE='40001';
  END IF;
  pre_root:=okg.external_action_restore_event_root(bound_deployment);
  IF pre_root->>'digest' IS DISTINCT FROM attestation_row.pre_restore_event_root_digest
     OR (pre_root->>'count')::bigint IS DISTINCT FROM attestation_row.pre_restore_event_head_count THEN
    RAISE EXCEPTION 'restore pre-event root or count is stale' USING ERRCODE='40001';
  END IF;
  IF (p_command->>'expected_inventory_count')::bigint<>attestation_row.effect_inventory_count
     OR p_command->>'expected_inventory_digest'<>attestation_row.effect_inventory_digest
     OR (p_command->>'expected_checkpoint_count')::bigint<>attestation_row.worker_checkpoint_count
     OR p_command->>'expected_checkpoint_set_digest'<>attestation_row.worker_checkpoint_set_digest THEN
    RAISE EXCEPTION 'restore attestation root or count disagrees with finalize request' USING ERRCODE='40001';
  END IF;
  inventory:=COALESCE(session_row.effect_inventory,'[]'::jsonb);
  inventory_count:=jsonb_array_length(inventory);
  expected_inventory_digest:=okg.external_action_restore_inventory_digest(inventory);
  IF inventory_count<>session_row.effect_inventory_count
     OR session_row.effect_inventory_digest IS DISTINCT FROM expected_inventory_digest THEN
    RAISE EXCEPTION 'restore database inventory projection is not canonical' USING ERRCODE='40001';
  END IF;
  SELECT count(*)::bigint,
         COALESCE(jsonb_agg(to_jsonb(effect_instance_key)
           ORDER BY convert_to(effect_instance_key,'UTF8')),'[]'::jsonb)
    INTO count_value,keys
    FROM okg.external_action_restore_checkpoint_entries
   WHERE deployment_name=bound_deployment AND restore_id=session_row.restore_id;
  IF count_value<>attestation_row.worker_checkpoint_count THEN
    RAISE EXCEPTION 'restore checkpoint coverage is incomplete or has an extra effect key'
      USING ERRCODE='40001',
        DETAIL=jsonb_build_object('code','incomplete_restore_coverage',
          'quarantine_state','blocked',
          'expected_checkpoint_count',attestation_row.worker_checkpoint_count,
          'accepted_checkpoint_count',count_value)::text;
  END IF;
  checkpoint_set:=okg.external_action_restore_checkpoint_set_digest(session_row.restore_id);
  expected_checkpoint_digest:=checkpoint_set->>'digest';
  IF checkpoint_set->>'count'<>(p_command->>'expected_checkpoint_count')
     OR expected_checkpoint_digest<>p_command->>'expected_checkpoint_set_digest' THEN
    RAISE EXCEPTION 'restore checkpoint set digest or count is not exact' USING ERRCODE='40001';
  END IF;
  IF EXISTS (
    SELECT 1 FROM okg.external_action_restore_checkpoint_entries e
     WHERE e.deployment_name=bound_deployment AND e.restore_id=session_row.restore_id
       AND (jsonb_typeof(e.checkpoint_payload) IS DISTINCT FROM 'object'
            OR e.checkpoint_payload->>'proof_kind' NOT IN ('positive','nonmembership'))
  ) THEN
    RAISE EXCEPTION 'restore checkpoint lacks a verified recovery projection' USING ERRCODE='40001';
  END IF;
  IF EXISTS (
    SELECT 1 FROM okg.external_action_restore_checkpoint_entries e
     WHERE e.deployment_name=bound_deployment AND e.restore_id=session_row.restore_id
       AND (
         e.checkpoint_payload->>'deployment' IS DISTINCT FROM bound_deployment
         OR e.checkpoint_payload->>'effect_instance_key' IS DISTINCT FROM e.effect_instance_key
         OR e.checkpoint_payload->>'execution_epoch' IS DISTINCT FROM attestation_row.old_epoch::text
         OR e.checkpoint_payload->>'ledger_fence_envelope_digest' IS DISTINCT FROM attestation_row.ledger_fence_envelope_digest
         OR e.checkpoint_payload->>'fenced_ledger_root_digest' IS DISTINCT FROM attestation_row.fenced_ledger_root_digest
         OR e.checkpoint_payload->>'fenced_ledger_entry_count' IS DISTINCT FROM attestation_row.fenced_ledger_entry_count::text
         OR e.checkpoint_payload->>'checkpoint_issued_at' IS NULL
         OR e.checkpoint_payload->>'submission_content_hash' IS NULL
         OR (e.checkpoint_payload->>'proof_kind'='positive'
             AND e.checkpoint_payload->>'positive_proof_digest' IS DISTINCT FROM e.proof_digest)
         OR (e.checkpoint_payload->>'proof_kind'='nonmembership'
             AND e.checkpoint_payload->>'nonmembership_proof_digest' IS DISTINCT FROM e.proof_digest)
       )
  ) THEN
    RAISE EXCEPTION 'restore checkpoint is not bound to the attested fence projection' USING ERRCODE='40001';
  END IF;
  IF EXISTS (
    SELECT 1 FROM okg.external_action_restore_checkpoint_entries e
     WHERE e.deployment_name=bound_deployment AND e.restore_id=session_row.restore_id
       AND e.checkpoint_payload->>'proof_kind'='positive'
       AND (e.leaf_index IS NULL OR e.leaf_index < 0)
  ) THEN
    RAISE EXCEPTION 'positive checkpoint index is not canonical decimal' USING ERRCODE='40001';
  END IF;
  SELECT count(*) FILTER (WHERE e.checkpoint_payload->>'proof_kind'='positive'),
         count(DISTINCT e.leaf_index) FILTER (WHERE e.checkpoint_payload->>'proof_kind'='positive'),
         min(e.leaf_index) FILTER (WHERE e.checkpoint_payload->>'proof_kind'='positive'),
         max(e.leaf_index) FILTER (WHERE e.checkpoint_payload->>'proof_kind'='positive')
    INTO positive_count,positive_distinct_count,positive_min,positive_max
    FROM okg.external_action_restore_checkpoint_entries e
   WHERE e.deployment_name=bound_deployment AND e.restore_id=session_row.restore_id;
  IF positive_count<>attestation_row.fenced_ledger_entry_count
     OR positive_distinct_count<>positive_count
     OR (positive_count>0 AND (positive_min<>0 OR positive_max<>positive_count-1)) THEN
    RAISE EXCEPTION 'positive checkpoint indexes do not cover the fenced ledger exactly' USING ERRCODE='40001';
  END IF;
  last_positive_key:=NULL;
  FOR positive_previous IN SELECT e.effect_instance_key FROM okg.external_action_restore_checkpoint_entries e
    WHERE e.deployment_name=bound_deployment AND e.restore_id=session_row.restore_id
      AND e.checkpoint_payload->>'proof_kind'='positive'
    ORDER BY e.leaf_index LOOP
    IF positive_previous IS NULL OR btrim(positive_previous)='' THEN
      RAISE EXCEPTION 'positive checkpoint has no effect key' USING ERRCODE='40001';
    END IF;
    IF last_positive_key IS NOT NULL AND convert_to(positive_previous,'UTF8')<=convert_to(last_positive_key,'UTF8') THEN
      RAISE EXCEPTION 'positive checkpoint ledger keys are not strictly sorted' USING ERRCODE='40001';
    END IF;
    last_positive_key:=positive_previous;
  END LOOP;
  SELECT COALESCE(jsonb_agg(to_jsonb(effect_instance_key)
      ORDER BY convert_to(effect_instance_key,'UTF8')),'[]'::jsonb)
    INTO db_inventory
    FROM (
      SELECT effect_instance_key FROM okg.external_action_proposals
       WHERE deployment_name=bound_deployment AND execution_epoch=attestation_row.old_epoch
      UNION
      SELECT d.effect_instance_key FROM okg.external_action_decisions d
       JOIN okg.external_action_proposals p ON p.deployment_name=d.deployment_name AND p.proposal_id=d.proposal_id
       WHERE d.deployment_name=bound_deployment AND p.execution_epoch=attestation_row.old_epoch
      UNION
      SELECT effect_instance_key FROM okg.external_action_executions
       WHERE deployment_name=bound_deployment AND execution_epoch=attestation_row.old_epoch
      UNION
      SELECT o.effect_instance_key FROM okg.external_action_outcomes o
       JOIN okg.external_action_executions e ON e.deployment_name=o.deployment_name AND e.execution_id=o.execution_id
       WHERE o.deployment_name=bound_deployment AND e.execution_epoch=attestation_row.old_epoch
      UNION
      SELECT effect_instance_key FROM okg.external_action_events
       WHERE deployment_name=bound_deployment AND execution_epoch=attestation_row.old_epoch
    ) database_inventory;
  IF db_inventory IS DISTINCT FROM inventory THEN
    RAISE EXCEPTION 'restore database inventory projection is not the retained old-epoch set' USING ERRCODE='40001';
  END IF;
  SELECT COALESCE(jsonb_agg(to_jsonb(effect_instance_key)
      ORDER BY convert_to(effect_instance_key,'UTF8')),'[]'::jsonb)
    INTO derived_inventory
    FROM (
      SELECT value #>> '{}' AS effect_instance_key
        FROM jsonb_array_elements(db_inventory) AS database_item(value)
      UNION
      SELECT e.effect_instance_key FROM okg.external_action_restore_checkpoint_entries e
       WHERE e.deployment_name=bound_deployment AND e.restore_id=session_row.restore_id
         AND e.checkpoint_payload->>'proof_kind'='positive'
    ) inventory_union;
  derived_inventory_count:=jsonb_array_length(derived_inventory);
  derived_inventory_digest:=okg.external_action_restore_inventory_digest(derived_inventory);
  IF derived_inventory_count<>attestation_row.effect_inventory_count
     OR derived_inventory_digest<>attestation_row.effect_inventory_digest
     OR count_value<>derived_inventory_count
     OR keys IS DISTINCT FROM derived_inventory THEN
    RAISE EXCEPTION 'restore inventory is not the database plus positive-ledger union' USING ERRCODE='40001';
  END IF;
  RETURN jsonb_build_object(
    'deployment',bound_deployment,'restore_id',session_row.restore_id,
    'restore_attestation_envelope_digest',attestation_row.envelope_digest,
    'effect_inventory_digest',attestation_row.effect_inventory_digest,
    'effect_inventory_count',attestation_row.effect_inventory_count,
    'worker_checkpoint_set_digest',attestation_row.worker_checkpoint_set_digest,
    'worker_checkpoint_count',attestation_row.worker_checkpoint_count,
    'pre_restore_event_root_digest',attestation_row.pre_restore_event_root_digest,
    'pre_restore_event_head_count',attestation_row.pre_restore_event_head_count
  );
END
$external_action_restore_assert_coverage$;

-- Final ACL convergence: internal external-action helpers are never a public
-- SQL surface.  The eight unrelated SECURITY DEFINER helpers are guarded
-- because this migration can run before the module that creates them.
DO $external_action_final_acl_internal$
BEGIN
  REVOKE ALL ON FUNCTION okg.external_action_session_deployment() FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_assert_deployment(TEXT) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_require_command(JSONB,TEXT[],TEXT[]) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_json_bytes(JSONB) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_hash(TEXT,JSONB) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_effect_key(JSONB) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_reject_mutation() FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_monotonic_nomos_updated_at() FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_nomos_authority_lock() FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_nomos_policy_bundle(TEXT,TEXT,TEXT) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_nomos_persist_decision(JSONB) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_record_auth_audit(JSONB) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_nomos_authority_snapshot(TEXT,TEXT,TEXT,TEXT,TEXT) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_startup_login_binding() FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_claim_owner_unavailability(TEXT) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_store_snapshot() FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_authorized_read(JSONB) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_store_resolve_principal(TEXT,TEXT) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_insert_dpop_replay_nonce(TEXT,TEXT,TEXT,TEXT,TEXT,TEXT,TEXT,TIMESTAMPTZ,TIMESTAMPTZ) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_store_startup(JSONB) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_store_insert_lease(JSONB) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_submit(JSONB) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_review_transition(JSONB) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_claim(JSONB) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_prepare_claim(JSONB) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_report_outcome(JSONB) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_owner_unavailability(JSONB) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_materialize_expiry(TEXT) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_restore_create(JSONB) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_restore_batch(JSONB) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_restore_prepare(JSONB) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_restore_finalize(JSONB) FROM PUBLIC;
  IF to_regprocedure('okg.external_action_restore_classification(jsonb,text)') IS NOT NULL THEN
    EXECUTE 'REVOKE ALL ON FUNCTION okg.external_action_restore_classification(JSONB,TEXT) FROM PUBLIC';
  END IF;
  IF to_regprocedure('okg.external_action_restore_build_plan(jsonb)') IS NOT NULL THEN
    EXECUTE 'REVOKE ALL ON FUNCTION okg.external_action_restore_build_plan(JSONB) FROM PUBLIC';
  END IF;
  IF to_regprocedure('okg.external_action_restore_inventory_digest(jsonb)') IS NOT NULL THEN
    EXECUTE 'REVOKE ALL ON FUNCTION okg.external_action_restore_inventory_digest(JSONB) FROM PUBLIC';
  END IF;
  IF to_regprocedure('okg.external_action_restore_inventory(text)') IS NOT NULL THEN
    EXECUTE 'REVOKE ALL ON FUNCTION okg.external_action_restore_inventory(TEXT) FROM PUBLIC';
  END IF;
  IF to_regprocedure('okg.external_action_restore_checkpoint_set_digest(text)') IS NOT NULL THEN
    EXECUTE 'REVOKE ALL ON FUNCTION okg.external_action_restore_checkpoint_set_digest(TEXT) FROM PUBLIC';
  END IF;
  IF to_regprocedure('okg.external_action_restore_event_root(text)') IS NOT NULL THEN
    EXECUTE 'REVOKE ALL ON FUNCTION okg.external_action_restore_event_root(TEXT) FROM PUBLIC';
  END IF;
  IF to_regprocedure('okg.external_action_restore_assert_coverage(jsonb)') IS NOT NULL THEN
    EXECUTE 'REVOKE ALL ON FUNCTION okg.external_action_restore_assert_coverage(JSONB) FROM PUBLIC';
  END IF;
  IF to_regprocedure('okg.external_action_restore_lifecycle_gate()') IS NOT NULL THEN
    EXECUTE 'REVOKE ALL ON FUNCTION okg.external_action_restore_lifecycle_gate() FROM PUBLIC';
  END IF;

  IF to_regprocedure('okg.mark_attribute_effective_integrity_dirty()') IS NOT NULL THEN
    EXECUTE 'REVOKE ALL ON FUNCTION okg.mark_attribute_effective_integrity_dirty() FROM PUBLIC';
  END IF;
  IF to_regprocedure('okg.mark_attribute_effective_integrity_dirty_rows()') IS NOT NULL THEN
    EXECUTE 'REVOKE ALL ON FUNCTION okg.mark_attribute_effective_integrity_dirty_rows() FROM PUBLIC';
  END IF;
  IF to_regprocedure('okg.mark_attribute_effective_integrity_dirty_update()') IS NOT NULL THEN
    EXECUTE 'REVOKE ALL ON FUNCTION okg.mark_attribute_effective_integrity_dirty_update() FROM PUBLIC';
  END IF;
  IF to_regprocedure('okg.require_chronos_checkout(text)') IS NOT NULL THEN
    EXECUTE 'REVOKE ALL ON FUNCTION okg.require_chronos_checkout(text) FROM PUBLIC';
  END IF;
  IF to_regprocedure('okg.require_graph_checkout(text)') IS NOT NULL THEN
    EXECUTE 'REVOKE ALL ON FUNCTION okg.require_graph_checkout(text) FROM PUBLIC';
  END IF;
  IF to_regprocedure('okg.runtime_queue_health(integer)') IS NOT NULL THEN
    EXECUTE 'REVOKE ALL ON FUNCTION okg.runtime_queue_health(integer) FROM PUBLIC';
  END IF;
  IF to_regprocedure('okg.source_frontier_active_workflows()') IS NOT NULL THEN
    EXECUTE 'REVOKE ALL ON FUNCTION okg.source_frontier_active_workflows() FROM PUBLIC';
  END IF;
  IF to_regprocedure('okg.source_frontier_state()') IS NOT NULL THEN
    EXECUTE 'REVOKE ALL ON FUNCTION okg.source_frontier_state() FROM PUBLIC';
  END IF;
END
$external_action_final_acl_internal$;

-- The coverage helper is defined after the historical grants above.  Keep it
-- owner-only just like the other restore planning/validation helpers; the API
-- can reach it only through prepare/finalize.
DO $external_action_restore_coverage_acl$
BEGIN
  REVOKE ALL ON FUNCTION okg.external_action_restore_assert_coverage(JSONB) FROM PUBLIC;
  IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname='okg_external_action_function_owner') THEN
    ALTER FUNCTION okg.external_action_restore_assert_coverage(JSONB) OWNER TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_assert_coverage(JSONB) TO okg_external_action_function_owner;
  END IF;
END
$external_action_restore_coverage_acl$;

CREATE OR REPLACE FUNCTION okg.external_action_restore_create(p_command JSONB)
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,okg
AS $external_action_restore_create_hardened$
DECLARE bound_deployment TEXT:=okg.external_action_session_deployment();
  session_row okg.external_action_restore_sessions%ROWTYPE;
  attestation_row okg.external_action_restore_attestations%ROWTYPE;
  fence_row okg.external_action_restore_fences%ROWTYPE;
  old_lease_row okg.external_action_execution_epoch_leases%ROWTYPE;
  new_lease_row okg.external_action_execution_epoch_leases%ROWTYPE;
  auth_row okg.external_action_auth_audits%ROWTYPE;
  completion_row okg.external_action_restore_completion_receipts%ROWTYPE;
  existing_attestation okg.external_action_restore_attestations%ROWTYPE;
  inventory_digest TEXT; expected_request_hash TEXT; evidence JSONB; evidence_item JSONB;
  evidence_name TEXT;
  observed_at TIMESTAMPTZ:=clock_timestamp();
BEGIN
  PERFORM okg.external_action_verifier_capability_status();
  PERFORM okg.external_action_require_command(
    p_command,
    ARRAY['deployment','restore_id','old_epoch','new_epoch','old_epoch_lease_digest',
      'new_epoch_lease_digest','ledger_fence_envelope_digest','fenced_ledger_root_digest',
      'fenced_ledger_entry_count','restored_snapshot_digest','pre_restore_event_root_digest',
      'pre_restore_event_head_count','effect_inventory','effect_inventory_digest',
      'effect_inventory_count','worker_checkpoint_set_digest','worker_checkpoint_count',
      'issued_at','expires_at','nonce','signer_separation_subject','protected_b64',
      'payload_b64','signature_b64','signer_key_id_digest','envelope_digest',
      'authentication_audit_id','nomos_decision_id','operator_separation_subject',
      'request_hash','idempotency_key_hash','old_epoch_lease_envelope_digest',
      'new_epoch_lease_envelope_digest','worker_quiescence_envelope_digest',
      'provider_authority_fence_envelope_digest','restore_evidence'],
    ARRAY['deployment','restore_id','old_epoch','new_epoch','old_epoch_lease_digest',
      'new_epoch_lease_digest','ledger_fence_envelope_digest','fenced_ledger_root_digest',
      'fenced_ledger_entry_count','restored_snapshot_digest','pre_restore_event_root_digest',
      'pre_restore_event_head_count','effect_inventory','effect_inventory_digest',
      'effect_inventory_count','worker_checkpoint_set_digest','worker_checkpoint_count',
      'issued_at','expires_at','nonce','signer_separation_subject','protected_b64',
      'payload_b64','signature_b64','signer_key_id_digest','envelope_digest',
      'authentication_audit_id','nomos_decision_id','operator_separation_subject',
      'request_hash','idempotency_key_hash','old_epoch_lease_envelope_digest',
      'new_epoch_lease_envelope_digest','worker_quiescence_envelope_digest',
      'provider_authority_fence_envelope_digest','restore_evidence']
  );
  IF p_command->>'deployment' IS DISTINCT FROM bound_deployment THEN
    RAISE EXCEPTION 'external action deployment is not bound to session_user' USING ERRCODE='28000';
  END IF;
  IF (p_command->>'new_epoch')::bigint <= (p_command->>'old_epoch')::bigint THEN
    RAISE EXCEPTION 'restore epoch must advance' USING ERRCODE='40001';
  END IF;
  inventory_digest:=okg.external_action_restore_inventory_digest(p_command->'effect_inventory');
  -- `effect_inventory` is the authoritative old-epoch database base returned
  -- by the inventory RPC.  Its digest/count are intentionally different
  -- from the signed attestation's final base UNION positive-ledger inventory;
  -- the latter is checked only after all checkpoint leaves are staged.
  -- The service has already verified the six detached envelopes.  SQL does
  -- not reimplement cryptography, but it does bind every retained evidence
  -- projection to the digest that was verified and to the attestation fields
  -- persisted below; an unbound evidence object cannot authorize restore.
  evidence:=p_command->'restore_evidence';
  IF jsonb_typeof(evidence) IS DISTINCT FROM 'object'
     OR (SELECT count(*) FROM jsonb_object_keys(evidence))<>6
     OR EXISTS (SELECT 1 FROM jsonb_object_keys(evidence) name
       WHERE name NOT IN ('old_epoch_lease','new_epoch_lease','worker_quiescence',
                          'provider_authority_fence','ledger_fence','restore_attestation')) THEN
    RAISE EXCEPTION 'restore evidence does not contain the closed six-envelope set' USING ERRCODE='22023';
  END IF;
  FOR evidence_name IN SELECT jsonb_object_keys(evidence) LOOP
    evidence_item:=evidence->evidence_name;
    IF jsonb_typeof(evidence_item) IS DISTINCT FROM 'object'
       OR EXISTS (SELECT 1 FROM jsonb_object_keys(evidence_item) field
          WHERE field NOT IN ('envelope','payload','envelope_digest'))
       OR NOT (evidence_item ? 'envelope') OR NOT (evidence_item ? 'payload')
       OR NOT (evidence_item ? 'envelope_digest')
       OR jsonb_typeof(evidence_item->'envelope') IS DISTINCT FROM 'object'
       OR jsonb_typeof(evidence_item->'payload') IS DISTINCT FROM 'object' THEN
      RAISE EXCEPTION 'restore evidence item is not a closed verified projection' USING ERRCODE='22023';
    END IF;
  END LOOP;
  IF evidence->'old_epoch_lease'->>'envelope_digest' IS DISTINCT FROM p_command->>'old_epoch_lease_envelope_digest'
     OR evidence->'new_epoch_lease'->>'envelope_digest' IS DISTINCT FROM p_command->>'new_epoch_lease_envelope_digest'
     OR evidence->'worker_quiescence'->>'envelope_digest' IS DISTINCT FROM p_command->>'worker_quiescence_envelope_digest'
     OR evidence->'provider_authority_fence'->>'envelope_digest' IS DISTINCT FROM p_command->>'provider_authority_fence_envelope_digest'
     OR evidence->'ledger_fence'->>'envelope_digest' IS DISTINCT FROM p_command->>'ledger_fence_envelope_digest'
     OR evidence->'restore_attestation'->>'envelope_digest' IS DISTINCT FROM p_command->>'envelope_digest' THEN
    RAISE EXCEPTION 'restore evidence envelope digest is not bound to the command' USING ERRCODE='40001';
  END IF;
  IF evidence->'restore_attestation'->'payload'->>'deployment' IS DISTINCT FROM p_command->>'deployment'
     OR evidence->'restore_attestation'->'payload'->>'restore_id' IS DISTINCT FROM p_command->>'restore_id'
     OR evidence->'restore_attestation'->'payload'->>'old_epoch' IS DISTINCT FROM p_command->>'old_epoch'
     OR evidence->'restore_attestation'->'payload'->>'new_epoch' IS DISTINCT FROM p_command->>'new_epoch'
     OR evidence->'restore_attestation'->'payload'->>'old_epoch_lease_digest' IS DISTINCT FROM p_command->>'old_epoch_lease_digest'
     OR evidence->'restore_attestation'->'payload'->>'new_epoch_lease_digest' IS DISTINCT FROM p_command->>'new_epoch_lease_digest'
     OR evidence->'restore_attestation'->'payload'->>'ledger_fence_envelope_digest' IS DISTINCT FROM p_command->>'ledger_fence_envelope_digest'
     OR evidence->'restore_attestation'->'payload'->>'fenced_ledger_root_digest' IS DISTINCT FROM p_command->>'fenced_ledger_root_digest'
     OR evidence->'restore_attestation'->'payload'->>'fenced_ledger_entry_count' IS DISTINCT FROM p_command->>'fenced_ledger_entry_count'
     OR evidence->'restore_attestation'->'payload'->>'restored_snapshot_digest' IS DISTINCT FROM p_command->>'restored_snapshot_digest'
     OR evidence->'restore_attestation'->'payload'->>'pre_restore_event_root_digest' IS DISTINCT FROM p_command->>'pre_restore_event_root_digest'
     OR evidence->'restore_attestation'->'payload'->>'pre_restore_event_head_count' IS DISTINCT FROM p_command->>'pre_restore_event_head_count'
     OR evidence->'restore_attestation'->'payload'->>'effect_inventory_digest' IS DISTINCT FROM p_command->>'effect_inventory_digest'
     OR evidence->'restore_attestation'->'payload'->>'effect_inventory_count' IS DISTINCT FROM p_command->>'effect_inventory_count'
     OR evidence->'restore_attestation'->'payload'->>'worker_checkpoint_set_digest' IS DISTINCT FROM p_command->>'worker_checkpoint_set_digest'
     OR evidence->'restore_attestation'->'payload'->>'worker_checkpoint_count' IS DISTINCT FROM p_command->>'worker_checkpoint_count'
     OR evidence->'restore_attestation'->'payload'->>'issued_at' IS DISTINCT FROM p_command->>'issued_at'
     OR evidence->'restore_attestation'->'payload'->>'expires_at' IS DISTINCT FROM p_command->>'expires_at'
     OR evidence->'restore_attestation'->'payload'->>'nonce' IS DISTINCT FROM p_command->>'nonce'
     OR evidence->'restore_attestation'->'payload'->>'signer_separation_subject' IS DISTINCT FROM p_command->>'signer_separation_subject' THEN
    RAISE EXCEPTION 'restore attestation projection is not bound to the command' USING ERRCODE='40001';
  END IF;
  expected_request_hash:=okg.external_action_hash(
    'okg-external-action-restore-create-request-v1',jsonb_build_object(
      'restore_id',p_command->>'restore_id',
      'old_epoch_lease_envelope_digest',p_command->>'old_epoch_lease_envelope_digest',
      'new_epoch_lease_envelope_digest',p_command->>'new_epoch_lease_envelope_digest',
      'worker_quiescence_envelope_digest',p_command->>'worker_quiescence_envelope_digest',
      'provider_authority_fence_envelope_digest',p_command->>'provider_authority_fence_envelope_digest',
      'ledger_fence_envelope_digest',p_command->>'ledger_fence_envelope_digest',
      'restore_attestation_envelope_digest',p_command->>'envelope_digest'));
  IF p_command->>'request_hash' IS DISTINCT FROM expected_request_hash THEN
    RAISE EXCEPTION 'restore create request hash is not canonical' USING ERRCODE='40001';
  END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended('okg.external_action.deployment:'||bound_deployment,0));
  PERFORM okg.external_action_nomos_authority_snapshot(bound_deployment,
    p_command->>'authentication_audit_id',p_command->>'nomos_decision_id','external_action.restore_reconcile',p_command->>'request_hash');
  SELECT * INTO auth_row FROM okg.external_action_auth_audits
   WHERE deployment_name=bound_deployment AND authentication_audit_id=p_command->>'authentication_audit_id';
  IF auth_row.separation_subject IS DISTINCT FROM p_command->>'operator_separation_subject'
     OR p_command->>'signer_separation_subject' IS DISTINCT FROM p_command->>'operator_separation_subject' THEN
    RAISE EXCEPTION 'restore operator and signed attestation subject are not bound' USING ERRCODE='42501';
  END IF;
  -- An exact create replay is allowed after finalization, when the normal
  -- fence is already back in service.  Return the durable completion receipt
  -- (or the immutable staging projection) instead of reporting the old
  -- mutable session status or attempting to create a second attestation.
  SELECT * INTO session_row FROM okg.external_action_restore_sessions
   WHERE deployment_name=bound_deployment AND restore_id=p_command->>'restore_id' FOR UPDATE;
  IF session_row.restore_session_row_id IS NOT NULL THEN
    IF session_row.request_hash IS DISTINCT FROM p_command->>'request_hash'
       OR session_row.idempotency_key_hash IS DISTINCT FROM p_command->>'idempotency_key_hash'
       OR session_row.operator_separation_subject IS DISTINCT FROM p_command->>'operator_separation_subject' THEN
      RAISE EXCEPTION 'restore id was reused with a different operator, key, or request' USING ERRCODE='40001';
    END IF;
    SELECT * INTO completion_row FROM okg.external_action_restore_completion_receipts
     WHERE deployment_name=bound_deployment AND restore_id=session_row.restore_id FOR SHARE;
    IF completion_row.completion_receipt_row_id IS NOT NULL THEN
      IF jsonb_typeof(completion_row.store_record) = 'object' THEN
        RETURN completion_row.store_record || jsonb_build_object('replay',TRUE);
      END IF;
      RETURN jsonb_build_object('replay',TRUE,'deployment',bound_deployment,
        'restore_id',session_row.restore_id,'state','complete',
        'quarantine_state','cleared','service_mode','normal',
        'restore_attestation_envelope_digest',session_row.restore_attestation_envelope_digest);
    END IF;
    SELECT * INTO attestation_row FROM okg.external_action_restore_attestations
     WHERE deployment_name=bound_deployment AND envelope_digest=session_row.restore_attestation_envelope_digest FOR SHARE;
    RETURN jsonb_build_object('replay',TRUE,'deployment',bound_deployment,
      'restore_id',session_row.restore_id,'state','collecting_checkpoints',
      'quarantine_state','blocked',
      'service_mode','restore_only',
      'restore_attestation_envelope_digest',session_row.restore_attestation_envelope_digest,
      'effect_inventory_digest',attestation_row.effect_inventory_digest,
      'effect_inventory_count',attestation_row.effect_inventory_count,
      'expected_checkpoint_set_digest',attestation_row.worker_checkpoint_set_digest,
      'expected_checkpoint_count',attestation_row.worker_checkpoint_count,
      'accepted_checkpoint_count',(
        SELECT count(*) FROM okg.external_action_restore_checkpoint_entries e
         WHERE e.deployment_name=bound_deployment AND e.restore_id=session_row.restore_id));
  END IF;
  SELECT * INTO fence_row FROM okg.external_action_restore_fences
   WHERE deployment_name=bound_deployment FOR UPDATE;
  IF fence_row.deployment_name IS NULL THEN
    RAISE EXCEPTION 'persisted startup restore fence is unavailable' USING ERRCODE='50300';
  END IF;
  SELECT * INTO old_lease_row
    FROM okg.external_action_execution_epoch_leases
   WHERE deployment_name=bound_deployment AND epoch=(p_command->>'old_epoch')::bigint
   ORDER BY lease_sequence DESC LIMIT 1;
  SELECT * INTO new_lease_row
    FROM okg.external_action_execution_epoch_leases
   WHERE deployment_name=bound_deployment AND epoch=(p_command->>'new_epoch')::bigint
   ORDER BY lease_sequence DESC LIMIT 1;
  IF old_lease_row.lease_row_id IS NULL
     OR new_lease_row.lease_row_id IS NULL
     OR old_lease_row.envelope_digest IS DISTINCT FROM p_command->>'old_epoch_lease_envelope_digest'
     OR new_lease_row.envelope_digest IS DISTINCT FROM p_command->>'new_epoch_lease_envelope_digest'
     OR old_lease_row.envelope_digest IS DISTINCT FROM p_command->>'old_epoch_lease_digest'
     OR new_lease_row.envelope_digest IS DISTINCT FROM p_command->>'new_epoch_lease_digest'
     OR fence_row.highest_accepted_epoch IS DISTINCT FROM new_lease_row.epoch
     OR fence_row.lease_digest IS DISTINCT FROM new_lease_row.envelope_digest
     OR fence_row.lease_chain_digest IS DISTINCT FROM new_lease_row.lease_chain_digest
     OR fence_row.verification_config_digest IS DISTINCT FROM new_lease_row.verification_config_digest
     OR fence_row.verification_login_binding_digest IS DISTINCT FROM new_lease_row.verification_login_binding_digest
     OR fence_row.service_mode <> 'restore_only'
     OR fence_row.quarantine_state <> 'blocked' THEN
    RAISE EXCEPTION 'restore epochs are not bound to the installed quarantined lease fence'
      USING ERRCODE='40001';
  END IF;
  SELECT * INTO existing_attestation FROM okg.external_action_restore_attestations
   WHERE deployment_name=bound_deployment AND envelope_digest=p_command->>'envelope_digest' FOR SHARE;
  IF existing_attestation.attestation_row_id IS NOT NULL THEN
    RAISE EXCEPTION 'restore attestation envelope digest was already used' USING ERRCODE='40001';
  END IF;
  INSERT INTO okg.external_action_restore_attestations (
    deployment_name,restore_id,old_epoch,new_epoch,old_epoch_lease_digest,new_epoch_lease_digest,
    ledger_fence_envelope_digest,fenced_ledger_root_digest,fenced_ledger_entry_count,
    restored_snapshot_digest,pre_restore_event_root_digest,pre_restore_event_head_count,
    effect_inventory_digest,effect_inventory_count,worker_checkpoint_set_digest,
    worker_checkpoint_count,issued_at,expires_at,nonce,signer_separation_subject,
    protected_b64,payload_b64,signature_b64,signer_key_id_digest,envelope_digest,
    verified_at,authentication_audit_id,nomos_decision_id,store_record
  ) VALUES (
    bound_deployment,p_command->>'restore_id',(p_command->>'old_epoch')::bigint,
    (p_command->>'new_epoch')::bigint,p_command->>'old_epoch_lease_digest',
    p_command->>'new_epoch_lease_digest',p_command->>'ledger_fence_envelope_digest',
    p_command->>'fenced_ledger_root_digest',(p_command->>'fenced_ledger_entry_count')::bigint,
    p_command->>'restored_snapshot_digest',p_command->>'pre_restore_event_root_digest',
    (p_command->>'pre_restore_event_head_count')::bigint,p_command->>'effect_inventory_digest',
    (p_command->>'effect_inventory_count')::bigint,p_command->>'worker_checkpoint_set_digest',
    (p_command->>'worker_checkpoint_count')::bigint,(p_command->>'issued_at')::timestamptz,
    (p_command->>'expires_at')::timestamptz,p_command->>'nonce',p_command->>'signer_separation_subject',
    p_command->>'protected_b64',p_command->>'payload_b64',p_command->>'signature_b64',
    p_command->>'signer_key_id_digest',p_command->>'envelope_digest',observed_at,
    p_command->>'authentication_audit_id',p_command->>'nomos_decision_id',p_command
  ) RETURNING * INTO attestation_row;
  INSERT INTO okg.external_action_restore_sessions (
    deployment_name,restore_id,request_hash,idempotency_key_hash,operator_separation_subject,
    authentication_audit_id,nomos_decision_id,restore_attestation_envelope_digest,
    status,created_at,updated_at,store_record,effect_inventory,effect_inventory_digest,
    effect_inventory_count,old_epoch_lease_envelope_digest,new_epoch_lease_envelope_digest,
    worker_quiescence_envelope_digest,provider_authority_fence_envelope_digest,
    ledger_fence_envelope_digest
  ) VALUES (
    bound_deployment,p_command->>'restore_id',p_command->>'request_hash',
    p_command->>'idempotency_key_hash',p_command->>'operator_separation_subject',
    p_command->>'authentication_audit_id',p_command->>'nomos_decision_id',attestation_row.envelope_digest,
    'staging',observed_at,observed_at,p_command,p_command->'effect_inventory',
    -- The session retains the deployment-owned database base inventory.  The
    -- signed attestation digest/count describe that base UNION the complete
    -- positive checkpoint leaf set and therefore cannot be used as the base
    -- projection's digest before checkpoint staging is complete.
    inventory_digest,jsonb_array_length(p_command->'effect_inventory'),
    COALESCE(p_command->>'old_epoch_lease_envelope_digest',p_command->>'old_epoch_lease_digest'),
    COALESCE(p_command->>'new_epoch_lease_envelope_digest',p_command->>'new_epoch_lease_digest'),
    p_command->>'worker_quiescence_envelope_digest',p_command->>'provider_authority_fence_envelope_digest',
    p_command->>'ledger_fence_envelope_digest'
  ) RETURNING * INTO session_row;
  UPDATE okg.external_action_restore_fences
     SET quarantine_state='blocked',service_mode='restore_only',
         restore_attestation_digest=attestation_row.envelope_digest,mode_changed_at=observed_at
   WHERE deployment_name=bound_deployment;
  RETURN jsonb_build_object('replay',FALSE,'deployment',bound_deployment,
    'restore_id',session_row.restore_id,'state','collecting_checkpoints',
    'quarantine_state','blocked','service_mode','restore_only',
    'restore_attestation_envelope_digest',session_row.restore_attestation_envelope_digest,
    'effect_inventory_digest',attestation_row.effect_inventory_digest,
    'effect_inventory_count',attestation_row.effect_inventory_count,
    'expected_checkpoint_set_digest',attestation_row.worker_checkpoint_set_digest,
    'expected_checkpoint_count',attestation_row.worker_checkpoint_count,
    'accepted_checkpoint_count',0);
END
$external_action_restore_create_hardened$;

DO $external_action_restore_hardened_grants$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname='okg_external_action_api') THEN
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_prepare(JSONB) TO okg_external_action_api;
  END IF;
  IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname='okg_external_action_function_owner') THEN
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_inventory_digest(JSONB) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_checkpoint_set_digest(TEXT) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_event_root(TEXT) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_assert_coverage(JSONB) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_prepare(JSONB) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_lifecycle_gate() TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_restore_inventory_digest(JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_restore_checkpoint_set_digest(TEXT) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_restore_event_root(TEXT) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_restore_assert_coverage(JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_restore_prepare(JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_restore_lifecycle_gate() OWNER TO okg_external_action_function_owner;
  END IF;
END
$external_action_restore_hardened_grants$;

DO $external_action_restore_hardened_acl$
BEGIN
  REVOKE ALL ON FUNCTION okg.external_action_restore_prepare(JSONB) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_restore_inventory_digest(JSONB) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_restore_checkpoint_set_digest(TEXT) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_restore_event_root(TEXT) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_restore_assert_coverage(JSONB) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_restore_lifecycle_gate() FROM PUBLIC;
  IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname='okg_external_action_api') THEN
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_prepare(JSONB) TO okg_external_action_api;
  END IF;
  IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname='okg_external_action_function_owner') THEN
    ALTER FUNCTION okg.external_action_restore_prepare(JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_restore_inventory_digest(JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_restore_checkpoint_set_digest(TEXT) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_restore_event_root(TEXT) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_restore_assert_coverage(JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_restore_lifecycle_gate() OWNER TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_prepare(JSONB) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_inventory_digest(JSONB) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_checkpoint_set_digest(TEXT) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_event_root(TEXT) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_assert_coverage(JSONB) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_lifecycle_gate() TO okg_external_action_function_owner;
  END IF;
END
$external_action_restore_hardened_acl$;

-- Derive the recovery class from the verified worker journal phase and the
-- proof kind.  `classification` is an internal projection and is never
-- accepted as authority from the worker payload.
CREATE OR REPLACE FUNCTION okg.external_action_restore_classification(
  p_payload JSONB, p_proof_kind TEXT
) RETURNS TEXT
LANGUAGE plpgsql IMMUTABLE SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $external_action_restore_classification$
DECLARE phase_value TEXT; outcome_value TEXT;
BEGIN
  IF jsonb_typeof(p_payload) IS DISTINCT FROM 'object'
     OR p_proof_kind NOT IN ('positive','nonmembership') THEN
    RAISE EXCEPTION 'restore checkpoint projection is invalid' USING ERRCODE='22023';
  END IF;
  phase_value:=p_payload->>'journal_phase';
  IF phase_value IN ('database_only_not_started','claimed_before_ledger_begin') THEN
    IF p_proof_kind<>'nonmembership'
       OR p_payload->>'positive_proof_digest' IS NOT NULL
       OR p_payload->>'ledger_entry_digest' IS NOT NULL
       OR p_payload->>'provider_idempotency_key_digest' IS NOT NULL
       OR p_payload->>'effect_started_at' IS NOT NULL
       OR p_payload->>'provider_receipt_digest' IS NOT NULL
       OR p_payload->>'outcome' IS NOT NULL
       OR p_payload->>'outcome_reason_code' IS NOT NULL
       OR p_payload->>'outcome_recorded_at' IS NOT NULL
       OR p_payload->>'journal_digest' IS NOT NULL THEN
      RAISE EXCEPTION 'database-only checkpoint has illegal ledger fields' USING ERRCODE='40001';
    END IF;
    RETURN 'database_restore_no_effect_started';
  END IF;
  IF phase_value NOT IN ('effect_starting','provider_receipt_observed','outcome_reported')
     OR p_proof_kind<>'positive'
     OR p_payload->>'nonmembership_proof_digest' IS NOT NULL
     OR p_payload->>'ledger_entry_digest' IS NULL
     OR p_payload->>'provider_idempotency_key_digest' IS NULL
     OR p_payload->>'effect_started_at' IS NULL
     OR p_payload->>'journal_digest' IS NULL THEN
    RAISE EXCEPTION 'restore checkpoint phase and proof combination is invalid' USING ERRCODE='40001';
  END IF;
  IF phase_value='effect_starting' THEN
    IF p_payload->>'provider_receipt_digest' IS NOT NULL OR p_payload->>'outcome' IS NOT NULL
       OR p_payload->>'outcome_reason_code' IS NOT NULL THEN
      RAISE EXCEPTION 'effect-starting checkpoint has terminal fields' USING ERRCODE='40001';
    END IF;
    RETURN 'database_restore_effect_uncertain';
  ELSIF phase_value='provider_receipt_observed' THEN
    IF p_payload->>'provider_receipt_digest' IS NULL OR p_payload->>'outcome' IS NOT NULL
       OR p_payload->>'outcome_reason_code' IS NOT NULL THEN
      RAISE EXCEPTION 'provider-receipt checkpoint has illegal outcome fields' USING ERRCODE='40001';
    END IF;
    -- PACT treats an authenticated provider receipt as authoritative
    -- completion.  Build-plan/finalize binds the reason to the retained
    -- action contract before persisting the succeeded outcome.
    RETURN 'database_restore_succeeded';
  END IF;
  outcome_value:=p_payload->>'outcome';
  IF outcome_value NOT IN ('succeeded','failed','indeterminate')
     OR p_payload->>'outcome_reason_code' IS NULL
     OR p_payload->>'outcome_recorded_at' IS NULL THEN
    RAISE EXCEPTION 'outcome checkpoint is invalid' USING ERRCODE='40001';
  END IF;
  IF outcome_value='succeeded' THEN
    IF p_payload->>'provider_receipt_digest' IS NULL THEN
      RAISE EXCEPTION 'successful checkpoint has no provider receipt' USING ERRCODE='40001';
    END IF;
    RETURN 'database_restore_succeeded';
  ELSIF outcome_value='failed' THEN
    -- Whether a failed provider outcome requires or forbids a receipt is an
    -- action-contract rule.  Build-plan revalidates that retained rule after
    -- resolving the proposal; the classifier must not reject a valid
    -- provider-declared failed outcome merely because it has a receipt.
    RETURN 'database_restore_failed';
  END IF;
  IF p_payload->>'provider_receipt_digest' IS NOT NULL THEN
    RAISE EXCEPTION 'indeterminate checkpoint has a provider receipt' USING ERRCODE='40001';
  END IF;
  RETURN 'database_restore_effect_uncertain';
END
$external_action_restore_classification$;

-- Build, but do not persist, the complete recovery/event plan.  Prepare and
-- finalize run on the same database transaction; the signed completion
-- receipt therefore covers this exact timestamp and these exact hashes.
CREATE OR REPLACE FUNCTION okg.external_action_restore_build_plan(p_command JSONB)
RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $external_action_restore_build_plan$
DECLARE
  bound_deployment TEXT:=okg.external_action_session_deployment();
  session_row okg.external_action_restore_sessions%ROWTYPE;
  attestation_row okg.external_action_restore_attestations%ROWTYPE;
  proposal_row okg.external_action_proposals%ROWTYPE;
  decision_row okg.external_action_decisions%ROWTYPE;
  execution_row okg.external_action_executions%ROWTYPE;
  outcome_row okg.external_action_outcomes%ROWTYPE;
  payload_row okg.external_action_payload_versions%ROWTYPE;
  entry RECORD; payload JSONB; checked JSONB;
  recovery_projection JSONB; event_projection JSONB; recovery_items JSONB:='[]'::jsonb;
  event_items JSONB:='[]'::jsonb; recovery_digests JSONB:='[]'::jsonb;
  classification_counts JSONB:='{}'::jsonb;
  pre_root JSONB; predicted_heads JSONB; new_head JSONB; post_root JSONB;
  recovery_set JSONB; coverage_command JSONB;
  source_proposal TEXT; source_decision TEXT; source_execution TEXT;
  classification TEXT; event_outcome TEXT; event_reason TEXT;
  recovered_outcome_value TEXT;
  before_review TEXT; after_review TEXT; before_execution TEXT; after_execution TEXT;
  submission_hash TEXT; previous_hash TEXT; event_hash_value TEXT;
  recovery_digest_value TEXT; previous_sequence BIGINT; event_sequence BIGINT;
  lifecycle_value BIGINT; payload_version_value BIGINT;
  effective_value TIMESTAMPTZ; completed_value TIMESTAMPTZ;
  actor_principal_value TEXT; policy_digest_value TEXT;
  decision_binding_value TEXT; execution_binding_value TEXT; outcome_request_value TEXT;
  projection_changed BOOLEAN; existing_outcome_value TEXT; contract_rule JSONB;
BEGIN
  PERFORM okg.external_action_require_command(
    p_command,
    ARRAY['deployment','restore_id','restore_attestation_envelope_digest',
      'expected_inventory_digest','expected_inventory_count',
      'expected_checkpoint_set_digest','expected_checkpoint_count',
      'completed_at','operator_separation_subject','idempotency_key_hash',
      'request_hash','authentication_audit_id','nomos_decision_id'],
    ARRAY['deployment','restore_id','restore_attestation_envelope_digest',
      'expected_inventory_digest','expected_inventory_count',
      'expected_checkpoint_set_digest','expected_checkpoint_count']
  );
  IF p_command->>'deployment' IS DISTINCT FROM bound_deployment THEN
    RAISE EXCEPTION 'external action deployment is not bound to session_user' USING ERRCODE='28000';
  END IF;
  coverage_command:=p_command-ARRAY['completed_at','operator_separation_subject','idempotency_key_hash','request_hash','authentication_audit_id','nomos_decision_id'];
  checked:=okg.external_action_restore_assert_coverage(coverage_command);
  SELECT * INTO session_row FROM okg.external_action_restore_sessions
    WHERE deployment_name=bound_deployment AND restore_id=p_command->>'restore_id' FOR UPDATE;
  SELECT * INTO attestation_row FROM okg.external_action_restore_attestations
    WHERE deployment_name=bound_deployment AND envelope_digest=session_row.restore_attestation_envelope_digest FOR SHARE;
  IF session_row.restore_session_row_id IS NULL OR attestation_row.attestation_row_id IS NULL THEN
    RAISE EXCEPTION 'restore plan has no immutable session or attestation' USING ERRCODE='40001';
  END IF;
  IF p_command ? 'authentication_audit_id' AND p_command->>'authentication_audit_id' IS DISTINCT FROM session_row.authentication_audit_id THEN
    RAISE EXCEPTION 'restore plan authentication audit is not session-bound' USING ERRCODE='42501';
  END IF;
  IF p_command ? 'nomos_decision_id' AND p_command->>'nomos_decision_id' IS DISTINCT FROM session_row.nomos_decision_id THEN
    RAISE EXCEPTION 'restore plan Nomos decision is not session-bound' USING ERRCODE='42501';
  END IF;
  IF clock_timestamp() >= attestation_row.expires_at THEN
    RAISE EXCEPTION 'restore attestation has expired' USING ERRCODE='40001';
  END IF;
  SELECT a.principal INTO actor_principal_value FROM okg.external_action_auth_audits a
   WHERE a.deployment_name=bound_deployment AND a.authentication_audit_id=session_row.authentication_audit_id
     AND a.separation_subject=session_row.operator_separation_subject;
  SELECT n.policy_fingerprint INTO policy_digest_value FROM okg.nomos_decisions n
   WHERE n.deployment_name=bound_deployment AND n.decision_id=session_row.nomos_decision_id;
  IF actor_principal_value IS NULL OR policy_digest_value IS NULL THEN
    RAISE EXCEPTION 'restore plan authentication or policy projection is unavailable' USING ERRCODE='42501';
  END IF;
  completed_value:=COALESCE(NULLIF(p_command->>'completed_at','')::timestamptz,clock_timestamp());
  pre_root:=okg.external_action_restore_event_root(bound_deployment);
  predicted_heads:=pre_root->'sorted_event_head_entries';
  FOR entry IN SELECT e.* FROM okg.external_action_restore_checkpoint_entries e
    WHERE e.deployment_name=bound_deployment AND e.restore_id=session_row.restore_id
    ORDER BY convert_to(e.effect_instance_key,'UTF8') LOOP
    payload:=entry.checkpoint_payload;
    IF jsonb_typeof(entry.proof) IS DISTINCT FROM 'object'
       OR entry.proof_digest IS DISTINCT FROM okg.external_action_hash(
         CASE WHEN payload->>'proof_kind'='positive'
              THEN 'okg-external-action-restore-positive-proof-v1'
              ELSE 'okg-external-action-restore-nonmembership-proof-v1' END,
         entry.proof) THEN
      RAISE EXCEPTION 'restore checkpoint proof digest is not bound to the retained verified proof'
        USING ERRCODE='40001';
    END IF;
    IF payload->>'proof_kind'='positive' THEN
      IF entry.proof->>'effect_instance_key' IS DISTINCT FROM entry.effect_instance_key
         OR entry.proof->>'ledger_entry_digest' IS DISTINCT FROM payload->>'ledger_entry_digest'
         OR entry.proof->>'leaf_count' IS DISTINCT FROM attestation_row.fenced_ledger_entry_count::text
         OR entry.leaf_index IS NULL
         OR entry.proof->>'leaf_index' IS DISTINCT FROM entry.leaf_index::text THEN
        RAISE EXCEPTION 'positive restore proof is not bound to the checkpoint ledger projection'
          USING ERRCODE='40001';
      END IF;
    ELSIF payload->>'proof_kind'='nonmembership' THEN
      IF entry.proof->>'effect_instance_key' IS DISTINCT FROM entry.effect_instance_key
         OR entry.proof->>'leaf_count' IS DISTINCT FROM attestation_row.fenced_ledger_entry_count::text THEN
        RAISE EXCEPTION 'nonmembership restore proof is not bound to the checkpoint ledger projection'
          USING ERRCODE='40001';
      END IF;
    END IF;
    classification:=okg.external_action_restore_classification(payload,payload->>'proof_kind');
    source_proposal:=NULLIF(payload->>'proposal_id','');
    source_decision:=NULLIF(payload->>'decision_id','');
    source_execution:=NULLIF(payload->>'execution_id','');
    -- A retained source row must carry its complete proposal/decision/claim
    -- chain.  When the restored snapshot did not retain those rows, a
    -- positive ledger leaf is still valid evidence for an orphan recovery
    -- tombstone; it must not be forced to invent source identifiers.
    IF (payload->>'proof_kind'='positive'
        OR payload->>'journal_phase'='claimed_before_ledger_begin')
       AND ((source_proposal IS NULL) <> (source_decision IS NULL)
            OR (source_decision IS NULL) <> (source_execution IS NULL)
            OR (source_proposal IS NOT NULL AND (
                 payload->>'proposal_request_hash' IS NULL
                 OR payload->>'original_payload_hash' IS NULL
                 OR payload->>'approved_payload_hash' IS NULL
                 OR payload->>'action_contract_digest' IS NULL
                 OR payload->>'decision_binding_hash' IS NULL
                 OR payload->>'execution_binding_hash' IS NULL
                 OR payload->>'okg_claim_handoff_receipt_digest' IS NULL
                 OR payload->>'pre_restore_effect_event_head_hash' IS NULL))) THEN
      RAISE EXCEPTION 'restore checkpoint has an incomplete OKG source chain' USING ERRCODE='40001';
    END IF;
    proposal_row:=NULL; decision_row:=NULL; execution_row:=NULL; outcome_row:=NULL;
    IF source_proposal IS NOT NULL THEN
      SELECT * INTO proposal_row FROM okg.external_action_proposals
       WHERE deployment_name=bound_deployment AND proposal_id=source_proposal FOR SHARE;
      IF proposal_row.proposal_id IS NULL OR proposal_row.effect_instance_key<>entry.effect_instance_key
         OR proposal_row.execution_epoch<>attestation_row.old_epoch THEN
        RAISE EXCEPTION 'restore checkpoint proposal binding is invalid' USING ERRCODE='40001';
      END IF;
      IF entry.effect_instance_key IS DISTINCT FROM okg.external_action_hash(
           'okg-external-action-effect-instance-v1',
           jsonb_build_object('deployment',bound_deployment,
             'proposer_separation_subject',proposal_row.proposer_separation_subject,
             'submission_idempotency_key_hash',proposal_row.submission_idempotency_key_hash))
         OR payload->>'submission_content_hash' IS DISTINCT FROM proposal_row.submission_content_hash
         OR payload->>'proposal_request_hash' IS DISTINCT FROM proposal_row.submission_request_hash
         OR payload->>'original_payload_hash' IS DISTINCT FROM proposal_row.original_payload_hash
         OR payload->>'action_contract_digest' IS DISTINCT FROM proposal_row.action_contract_digest
         OR payload->>'submission_content_hash' IS DISTINCT FROM okg.external_action_hash(
           'okg-external-action-submission-content-v1',jsonb_build_object(
             'automation_id',proposal_row.automation_id,'automation_version',proposal_row.automation_version,
             'run_id',proposal_row.run_id,'correlation_id',proposal_row.correlation_id,
             'action_type',proposal_row.action_type,'target_ref',proposal_row.target_ref,
             'risk_tier',proposal_row.risk_tier,'action_schema_digest',proposal_row.action_schema_digest,
             'action_contract_digest',proposal_row.action_contract_digest,
             'original_payload_hash',proposal_row.original_payload_hash,
             'requested_review_expires_at',to_char(proposal_row.requested_review_expires_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
             'summary_code',proposal_row.summary_code,'evidence_digests',proposal_row.evidence_digests)) THEN
        RAISE EXCEPTION 'restore checkpoint proposal semantic hash is invalid' USING ERRCODE='40001';
      END IF;
      SELECT * INTO payload_row FROM okg.external_action_payload_versions
       WHERE deployment_name=bound_deployment AND proposal_id=proposal_row.proposal_id
       ORDER BY payload_version ASC LIMIT 1 FOR SHARE;
      IF payload_row.payload_version_id IS NULL
         OR payload_row.payload_hash IS DISTINCT FROM proposal_row.original_payload_hash
         OR payload_row.payload_hash IS DISTINCT FROM okg.external_action_hash(
           'okg-external-action-payload-v1',convert_from(payload_row.canonical_payload,'UTF8')::jsonb) THEN
        RAISE EXCEPTION 'restore checkpoint original payload hash is invalid' USING ERRCODE='40001';
      END IF;
      IF payload->>'journal_phase'='outcome_reported' THEN
        contract_rule:=proposal_row.action_contract_record->'outcome_reason_rules'->(payload->>'outcome_reason_code');
        IF jsonb_typeof(proposal_row.action_contract_record) IS DISTINCT FROM 'object'
           OR jsonb_typeof(proposal_row.action_contract_record->'outcome_reason_rules') IS DISTINCT FROM 'object'
           OR jsonb_typeof(contract_rule) IS DISTINCT FROM 'object'
           OR jsonb_typeof(contract_rule->'allowed_outcomes') IS DISTINCT FROM 'array'
           OR NOT COALESCE(contract_rule->'allowed_outcomes' ? (payload->>'outcome'),FALSE)
           OR contract_rule->>'provider_receipt' IS NULL
           OR contract_rule->>'provider_receipt' NOT IN ('required','forbidden')
           OR ((contract_rule->>'provider_receipt')='required') IS DISTINCT FROM (payload->>'provider_receipt_digest' IS NOT NULL) THEN
          RAISE EXCEPTION 'restore checkpoint outcome is not allowed by the retained action contract' USING ERRCODE='40001';
        END IF;
      END IF;
    END IF;
    IF source_decision IS NOT NULL THEN
      IF source_proposal IS NULL THEN
        RAISE EXCEPTION 'restore checkpoint decision has no proposal' USING ERRCODE='40001';
      END IF;
      SELECT * INTO decision_row FROM okg.external_action_decisions
       WHERE deployment_name=bound_deployment AND decision_id=source_decision FOR SHARE;
      IF decision_row.decision_id IS NULL OR decision_row.proposal_id<>source_proposal
         OR decision_row.effect_instance_key<>entry.effect_instance_key
         OR decision_row.execution_epoch<>attestation_row.old_epoch THEN
        RAISE EXCEPTION 'restore checkpoint decision binding is invalid' USING ERRCODE='40001';
      END IF;
      decision_binding_value:=okg.external_action_hash(
        'okg-external-action-decision-binding-v1',jsonb_build_object(
          'deployment',bound_deployment,'execution_epoch',decision_row.execution_epoch,
          'effect_instance_key',decision_row.effect_instance_key,'proposal_id',decision_row.proposal_id,
          'decision_id',decision_row.decision_id,
          'approved_lifecycle_version',decision_row.approved_lifecycle_version,
          'approved_payload_version',decision_row.approved_payload_version,
          'approved_payload_hash',decision_row.approved_payload_hash,
          'action_type',decision_row.action_type,'target_ref',decision_row.target_ref,
          'action_schema_digest',decision_row.action_schema_digest,
          'action_contract_digest',decision_row.action_contract_digest,
          'executor_contract_digest',decision_row.executor_contract_digest,
          'payload_renderer_digest',decision_row.payload_renderer_digest,
          'governed_tool_binding_digest',decision_row.governed_tool_binding_digest,
          'provider_connection_contract_digest',decision_row.provider_connection_contract_digest,
          'provider_capability_digest',decision_row.provider_capability_digest,
          'provider_operation_id',decision_row.provider_operation_id,
          'provider_operation_descriptor_digest',decision_row.provider_operation_descriptor_digest,
          'policy_digest',decision_row.policy_digest,
          'execute_before',CASE WHEN decision_row.execute_before IS NULL THEN NULL ELSE to_char(decision_row.execute_before AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"') END));
      IF decision_row.decision_binding_hash IS DISTINCT FROM decision_binding_value
         OR payload->>'decision_binding_hash' IS DISTINCT FROM decision_binding_value
         OR payload->>'approved_payload_hash' IS DISTINCT FROM decision_row.approved_payload_hash THEN
        RAISE EXCEPTION 'restore checkpoint decision semantic hash is invalid' USING ERRCODE='40001';
      END IF;
    END IF;
    IF source_proposal IS NULL AND (
         payload->>'proposal_request_hash' IS NOT NULL
         OR payload->>'original_payload_hash' IS NOT NULL
         OR payload->>'action_contract_digest' IS NOT NULL
         OR payload->>'approved_payload_hash' IS NOT NULL) THEN
      RAISE EXCEPTION 'orphan restore checkpoint invents proposal fields' USING ERRCODE='40001';
    END IF;
    IF source_decision IS NULL AND (
         payload->>'decision_binding_hash' IS NOT NULL
         OR payload->>'approved_payload_hash' IS NOT NULL) THEN
      RAISE EXCEPTION 'restore checkpoint invents decision fields' USING ERRCODE='40001';
    END IF;
    IF source_execution IS NULL AND (
         payload->>'execution_binding_hash' IS NOT NULL
         OR payload->>'okg_claim_handoff_receipt_digest' IS NOT NULL
         OR payload->>'provider_idempotency_key_digest' IS NOT NULL) THEN
      RAISE EXCEPTION 'restore checkpoint invents execution fields' USING ERRCODE='40001';
    END IF;
    IF source_execution IS NOT NULL THEN
      IF source_proposal IS NULL OR source_decision IS NULL THEN
        RAISE EXCEPTION 'restore checkpoint execution has incomplete source chain' USING ERRCODE='40001';
      END IF;
      SELECT * INTO execution_row FROM okg.external_action_executions
       WHERE deployment_name=bound_deployment AND execution_id=source_execution FOR SHARE;
      IF execution_row.execution_id IS NULL OR execution_row.proposal_id<>source_proposal
         OR execution_row.decision_id<>source_decision
         OR execution_row.effect_instance_key<>entry.effect_instance_key
         OR execution_row.execution_epoch<>attestation_row.old_epoch THEN
        RAISE EXCEPTION 'restore checkpoint execution binding is invalid' USING ERRCODE='40001';
      END IF;
      execution_binding_value:=okg.external_action_hash(
        'okg-external-action-execution-binding-v1',jsonb_build_object(
          'deployment',bound_deployment,'execution_epoch',execution_row.execution_epoch,
          'execution_epoch_lease_digest',execution_row.execution_epoch_lease_digest,
          'effect_instance_key',execution_row.effect_instance_key,'proposal_id',execution_row.proposal_id,
          'decision_id',execution_row.decision_id,'execution_id',execution_row.execution_id,
          'worker_separation_subject',execution_row.worker_separation_subject,
          'approved_lifecycle_version',execution_row.approved_lifecycle_version,
          'approved_payload_version',execution_row.approved_payload_version,
          'approved_payload_hash',execution_row.approved_payload_hash,
          'action_type',execution_row.action_type,'target_ref',execution_row.target_ref,
          'action_schema_digest',execution_row.action_schema_digest,
          'action_contract_digest',execution_row.action_contract_digest,
          'executor_contract_digest',execution_row.executor_contract_digest,
          'payload_renderer_digest',execution_row.payload_renderer_digest,
          'governed_tool_binding_digest',execution_row.governed_tool_binding_digest,
          'provider_connection_contract_digest',execution_row.provider_connection_contract_digest,
          'provider_capability_digest',execution_row.provider_capability_digest,
          'provider_operation_id',execution_row.provider_operation_id,
          'provider_operation_descriptor_digest',execution_row.provider_operation_descriptor_digest,
          'policy_digest',execution_row.policy_digest));
      IF execution_row.execution_binding_hash IS DISTINCT FROM execution_binding_value
         OR payload->>'execution_binding_hash' IS DISTINCT FROM execution_binding_value
         OR payload->>'okg_claim_handoff_receipt_digest' IS DISTINCT FROM execution_row.okg_claim_handoff_receipt_digest
         OR payload->>'provider_idempotency_key_digest' IS DISTINCT FROM execution_row.provider_idempotency_key_digest THEN
        RAISE EXCEPTION 'restore checkpoint execution semantic hash is invalid' USING ERRCODE='40001';
      END IF;
      SELECT * INTO outcome_row FROM okg.external_action_outcomes
       WHERE deployment_name=bound_deployment AND execution_id=source_execution FOR SHARE;
      IF outcome_row.outcome_id IS NOT NULL THEN
        outcome_request_value:=okg.external_action_hash(
          'okg-external-action-outcome-request-v1',jsonb_build_object(
            'execution_id',outcome_row.execution_id,'effect_instance_key',outcome_row.effect_instance_key,
            'worker_or_reconciler_separation_subject',outcome_row.worker_or_reconciler_separation_subject,
            'outcome',outcome_row.outcome,'outcome_reason_code',outcome_row.outcome_reason_code,
            'provider_receipt_digest',outcome_row.provider_receipt_digest,
            'recovery_journal_digest',outcome_row.recovery_journal_digest,
            'owner_unavailability_attestation_digest',outcome_row.owner_unavailability_attestation_digest));
        IF outcome_row.request_hash IS DISTINCT FROM outcome_request_value THEN
          RAISE EXCEPTION 'restore checkpoint outcome semantic hash is invalid' USING ERRCODE='40001';
        END IF;
        IF payload->>'outcome' IS NOT NULL AND (
             payload->>'outcome' IS DISTINCT FROM outcome_row.outcome
             OR payload->>'outcome_reason_code' IS DISTINCT FROM outcome_row.outcome_reason_code
             OR payload->>'provider_receipt_digest' IS DISTINCT FROM outcome_row.provider_receipt_digest
             OR payload->>'journal_digest' IS DISTINCT FROM outcome_row.recovery_journal_digest) THEN
          RAISE EXCEPTION 'restore reconciliation is terminal: checkpoint conflicts with existing terminal outcome'
            USING ERRCODE='40001',
              DETAIL=jsonb_build_object('code','restore_reconciliation_conflict',
                'classification','database_restore_terminal_conflict',
                'effect_instance_key',entry.effect_instance_key,
                'retained_outcome',outcome_row.outcome,
                'signed_outcome',payload->>'outcome',
                'retained_outcome_reason_code',outcome_row.outcome_reason_code,
                'signed_outcome_reason_code',payload->>'outcome_reason_code')::text;
        END IF;
      END IF;
    END IF;
    SELECT COALESCE(max(e.sequence),0),
           (array_agg(e.event_hash ORDER BY e.sequence))[array_length(array_agg(e.event_hash ORDER BY e.sequence),1)]
      INTO previous_sequence,previous_hash FROM okg.external_action_events e
     WHERE e.deployment_name=bound_deployment AND e.effect_instance_key=entry.effect_instance_key;
    IF payload->>'pre_restore_effect_event_head_hash' IS DISTINCT FROM previous_hash THEN
      RAISE EXCEPTION 'restore checkpoint event head is stale' USING ERRCODE='40001';
    END IF;
    effective_value:=COALESCE(NULLIF(payload->>'effect_started_at','')::timestamptz,
      NULLIF(payload->>'outcome_recorded_at','')::timestamptz,
      NULLIF(payload->>'checkpoint_issued_at','')::timestamptz);
    IF effective_value IS NULL THEN
      RAISE EXCEPTION 'restore checkpoint has no signed effective time' USING ERRCODE='40001';
    END IF;
    before_review:=proposal_row.review_state; before_execution:=proposal_row.execution_state;
    after_review:=before_review; after_execution:=before_execution;
    lifecycle_value:=COALESCE(proposal_row.lifecycle_version,1);
    payload_version_value:=proposal_row.payload_version;
    event_outcome:=NULL; event_reason:=classification; projection_changed:=FALSE;
    IF classification='database_restore_no_effect_started' THEN
      IF proposal_row.proposal_id IS NOT NULL AND source_execution IS NULL
         AND before_review IN ('rejected','expired','cancelled') THEN
        classification:='database_restore_terminal_review_confirmed';
      ELSIF proposal_row.proposal_id IS NOT NULL THEN
        after_review:='cancelled'; after_execution:='unavailable'; projection_changed:=TRUE;
      END IF;
    ELSIF classification IN ('database_restore_effect_uncertain','database_restore_succeeded','database_restore_failed') THEN
      IF outcome_row.outcome_id IS NOT NULL THEN
        IF payload->>'outcome' IS NOT NULL AND outcome_row.outcome<>payload->>'outcome' THEN
          RAISE EXCEPTION 'restore reconciliation is terminal: checkpoint conflicts with existing terminal outcome'
            USING ERRCODE='40001',
              DETAIL=jsonb_build_object('code','restore_reconciliation_conflict',
                'classification','database_restore_terminal_conflict',
                'effect_instance_key',entry.effect_instance_key,
                'retained_outcome',outcome_row.outcome,
                'signed_outcome',payload->>'outcome')::text;
        END IF;
        classification:='database_restore_terminal_confirmed';
        event_outcome:=outcome_row.outcome;
      ELSIF proposal_row.proposal_id IS NOT NULL THEN
        after_review:='approved';
        after_execution:=CASE classification WHEN 'database_restore_succeeded' THEN 'succeeded' WHEN 'database_restore_failed' THEN 'failed' ELSE 'indeterminate' END;
        projection_changed:=TRUE;
        event_outcome:=CASE classification WHEN 'database_restore_succeeded' THEN 'succeeded' WHEN 'database_restore_failed' THEN 'failed' ELSE COALESCE(payload->>'outcome','indeterminate') END;
      ELSE
        event_outcome:=CASE classification WHEN 'database_restore_succeeded' THEN 'succeeded' WHEN 'database_restore_failed' THEN 'failed' ELSE COALESCE(payload->>'outcome','indeterminate') END;
      END IF;
    END IF;
    -- The database projection may refine a journal-derived class (for
    -- example, a surviving terminal review or an already recorded outcome).
    -- The event reason must name that final class, never the unrefined input.
    event_reason:=classification;
    IF classification='database_restore_terminal_confirmed' AND event_outcome IS NULL THEN
      event_outcome:=outcome_row.outcome;
    END IF;
    -- The recovery projection records the durable outcome selected by the
    -- classifier.  In particular, an uncertain ledger phase is an explicit
    -- indeterminate outcome, and a matching pre-existing outcome must not be
    -- lost merely because the signed checkpoint omitted its outcome fields.
    recovered_outcome_value:=COALESCE(outcome_row.outcome,event_outcome,NULLIF(payload->>'outcome',''));
    classification_counts:=classification_counts||jsonb_build_object(
      classification,COALESCE((classification_counts->>classification)::bigint,0)+1);
    IF projection_changed THEN lifecycle_value:=lifecycle_value+1; END IF;
    submission_hash:=COALESCE(proposal_row.submission_content_hash,NULLIF(payload->>'submission_content_hash',''));
    IF submission_hash IS NULL THEN
      RAISE EXCEPTION 'restore checkpoint has no submission content binding' USING ERRCODE='40001';
    END IF;
    recovery_projection:=jsonb_build_object(
      'deployment',bound_deployment,'old_epoch',attestation_row.old_epoch,'new_epoch',attestation_row.new_epoch,
      'effect_instance_key',entry.effect_instance_key,'submission_content_hash',submission_hash,
      'checkpoint_digest',entry.checkpoint_envelope_digest,'restore_attestation_digest',attestation_row.envelope_digest,
      'classification',classification,'source_proposal_id',source_proposal,'source_decision_id',source_decision,
      'source_execution_id',source_execution,'recovered_review_state',after_review,
      'recovered_execution_state',after_execution,'recovered_outcome',recovered_outcome_value,
      'effective_at',to_char(effective_value AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
      'recorded_at',to_char(completed_value AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
      'pre_restore_effect_event_head_hash',previous_hash);
    recovery_digest_value:=okg.external_action_hash('okg-external-action-restore-recovery-v1',recovery_projection);
    event_sequence:=previous_sequence+1;
    event_projection:=jsonb_build_object(
      'proposal_id',source_proposal,'execution_epoch',attestation_row.new_epoch,
      'effect_instance_key',entry.effect_instance_key,'sequence',event_sequence,
      'lifecycle_version',lifecycle_value,'event_type','restore_recovered',
      'actor_kind','authenticated_principal','actor_principal',actor_principal_value,
      'actor_separation_subject',session_row.operator_separation_subject,
      'authentication_audit_ref',session_row.authentication_audit_id,
      'nomos_decision_ref',session_row.nomos_decision_id,'deadline_kind',NULL::TEXT,
      'policy_digest',policy_digest_value,'before_review_state',before_review,
      'after_review_state',after_review,'before_execution_state',before_execution,
      'after_execution_state',after_execution,'payload_version',payload_version_value,
      'payload_hash',proposal_row.current_payload_hash,'decision_id',source_decision,
      'decision_binding_hash',decision_row.decision_binding_hash,'execution_id',source_execution,
      'execution_binding_hash',execution_row.execution_binding_hash,'transition_reason_code',NULL::TEXT,
      'outcome',event_outcome,'outcome_reason_code',event_reason,'outcome_request_hash',NULL::TEXT,
      'restore_recovery_digest',recovery_digest_value,
      'effective_at',to_char(effective_value AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
      'recorded_at',to_char(completed_value AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
      'previous_event_hash',previous_hash);
    event_hash_value:=okg.external_action_hash('okg-external-action-event-v1',event_projection);
    recovery_items:=recovery_items||jsonb_build_array(recovery_projection||jsonb_build_object(
      'recovery_digest',recovery_digest_value,'restored_effect_event_head_hash',event_hash_value));
    event_items:=event_items||jsonb_build_array(event_projection||jsonb_build_object('event_hash',event_hash_value,'projection_changed',projection_changed));
    recovery_digests:=recovery_digests||jsonb_build_array(to_jsonb(recovery_digest_value));
    new_head:=jsonb_build_object('effect_instance_key',entry.effect_instance_key,'sequence',event_sequence,'event_hash',event_hash_value);
    SELECT COALESCE(jsonb_agg(h ORDER BY convert_to(h->>'effect_instance_key','UTF8')),'[]'::jsonb)
      INTO predicted_heads FROM jsonb_array_elements(predicted_heads) h
     WHERE h->>'effect_instance_key'<>entry.effect_instance_key;
    predicted_heads:=predicted_heads||jsonb_build_array(new_head);
    SELECT COALESCE(jsonb_agg(h ORDER BY convert_to(h->>'effect_instance_key','UTF8')),'[]'::jsonb)
      INTO predicted_heads FROM jsonb_array_elements(predicted_heads) h;
  END LOOP;
  SELECT COALESCE(jsonb_agg(d ORDER BY convert_to(d #>> '{}','UTF8')),'[]'::jsonb)
    INTO recovery_digests FROM jsonb_array_elements(recovery_digests) d;
  recovery_set:=jsonb_build_object('sorted_restore_recovery_digests',recovery_digests,'count',jsonb_array_length(recovery_digests));
  recovery_set:=recovery_set||jsonb_build_object('digest',okg.external_action_hash('okg-external-action-restore-recovery-set-v1',recovery_set));
  post_root:=jsonb_build_object('sorted_event_head_entries',predicted_heads,'count',jsonb_array_length(predicted_heads));
  post_root:=post_root||jsonb_build_object('digest',okg.external_action_hash('okg-external-action-restore-event-root-v1',post_root));
  RETURN jsonb_build_object(
    'projection',jsonb_build_object('deployment',bound_deployment,'restore_id',session_row.restore_id,
      'restore_attestation_envelope_digest',attestation_row.envelope_digest,
      'pre_restore_event_root_digest',pre_root->>'digest','pre_restore_event_head_count',(pre_root->>'count')::bigint,
      'restored_event_root_digest',post_root->>'digest','restored_event_head_count',(post_root->>'count')::bigint,
      'recovery_set_digest',recovery_set->>'digest','recovery_count',(recovery_set->>'count')::bigint,
      'completed_at',to_char(completed_value AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"')),
    'recoveries',recovery_items,'events',event_items,
    'classification_summary',classification_counts);
END
$external_action_restore_build_plan$;

-- Prepare is a read/plan operation.  A terminal idempotent replay is checked
-- before the restore-only guard so an exact committed replay remains safe
-- after the deployment has returned to normal service.
CREATE OR REPLACE FUNCTION okg.external_action_restore_prepare(p_command JSONB)
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,okg
AS $external_action_restore_prepare_final$
DECLARE bound_deployment TEXT:=okg.external_action_session_deployment();
  session_row okg.external_action_restore_sessions%ROWTYPE;
  existing_final okg.external_action_restore_finalize_idempotency%ROWTYPE;
  plan JSONB; plan_command JSONB; operator_value TEXT; key_value TEXT; request_value TEXT;
  expected_request_hash TEXT;
BEGIN
  PERFORM okg.external_action_verifier_capability_status();
  PERFORM okg.external_action_require_command(p_command,
    ARRAY['deployment','restore_id','restore_attestation_envelope_digest','expected_inventory_digest',
      'expected_inventory_count','expected_checkpoint_set_digest','expected_checkpoint_count',
      'operator_separation_subject','idempotency_key_hash','request_hash','authentication_audit_id','nomos_decision_id'],
    ARRAY['deployment','restore_id','restore_attestation_envelope_digest','expected_inventory_digest',
      'expected_inventory_count','expected_checkpoint_set_digest','expected_checkpoint_count',
      'operator_separation_subject','idempotency_key_hash','request_hash']);
  IF p_command->>'deployment' IS DISTINCT FROM bound_deployment THEN
    RAISE EXCEPTION 'external action deployment is not bound to session_user' USING ERRCODE='28000';
  END IF;
  operator_value:=p_command->>'operator_separation_subject'; key_value:=p_command->>'idempotency_key_hash'; request_value:=p_command->>'request_hash';
  -- Resolve the immutable session and current authority before the terminal
  -- replay lookup.  A matching key cannot bypass a revoked audit/decision,
  -- while an exact replay remains independent of the current service mode.
  PERFORM pg_advisory_xact_lock(hashtextextended('okg.external_action.deployment:'||bound_deployment,0));
  SELECT * INTO session_row FROM okg.external_action_restore_sessions
   WHERE deployment_name=bound_deployment AND restore_id=p_command->>'restore_id' FOR SHARE;
  IF session_row.restore_session_row_id IS NULL OR session_row.operator_separation_subject IS DISTINCT FROM operator_value THEN
    RAISE EXCEPTION 'restore plan operator is not session-bound' USING ERRCODE='42501';
  END IF;
  IF session_row.authentication_audit_id IS DISTINCT FROM p_command->>'authentication_audit_id'
     OR session_row.nomos_decision_id IS DISTINCT FROM p_command->>'nomos_decision_id' THEN
    RAISE EXCEPTION 'restore plan authority is not session-bound' USING ERRCODE='42501';
  END IF;
  PERFORM okg.external_action_nomos_authority_snapshot(bound_deployment,p_command->>'authentication_audit_id',p_command->>'nomos_decision_id','external_action.restore_reconcile',p_command->>'request_hash');
  SELECT * INTO existing_final FROM okg.external_action_restore_finalize_idempotency
   WHERE deployment_name=bound_deployment AND restore_id=p_command->>'restore_id' FOR SHARE;
  IF existing_final.finalize_idempotency_row_id IS NOT NULL THEN
    IF existing_final.operator_separation_subject IS DISTINCT FROM operator_value
       OR existing_final.idempotency_key_hash IS DISTINCT FROM key_value
       OR existing_final.request_hash IS DISTINCT FROM request_value THEN
      RAISE EXCEPTION 'restore finalize replay conflicts with the first operator, key, or request' USING ERRCODE='40001';
    END IF;
    RETURN jsonb_build_object('replay',TRUE,'response_body_jcs',jsonb_build_object('__okg_bytes_b64',encode(existing_final.response_body_jcs,'base64')),'completion_receipt_envelope_digest',existing_final.completion_receipt_envelope_digest);
  END IF;
  expected_request_hash:=okg.external_action_hash(
    'okg-external-action-restore-finalize-request-v1',jsonb_build_object(
      'restore_attestation_envelope_digest',p_command->>'restore_attestation_envelope_digest',
      'expected_inventory_digest',p_command->>'expected_inventory_digest',
      'expected_inventory_count',(p_command->>'expected_inventory_count')::bigint,
      'expected_checkpoint_set_digest',p_command->>'expected_checkpoint_set_digest',
      'expected_checkpoint_count',(p_command->>'expected_checkpoint_count')::bigint));
  IF request_value IS DISTINCT FROM expected_request_hash THEN
    RAISE EXCEPTION 'restore finalize request hash is not canonical' USING ERRCODE='40001';
  END IF;
  SELECT * INTO session_row FROM okg.external_action_restore_sessions
   WHERE deployment_name=bound_deployment AND restore_id=p_command->>'restore_id' FOR UPDATE;
  plan_command:=p_command-ARRAY['operator_separation_subject','idempotency_key_hash','request_hash'];
  plan:=okg.external_action_restore_build_plan(plan_command);
  RETURN (plan->'projection') || jsonb_build_object('replay',FALSE);
END
$external_action_restore_prepare_final$;

-- Finalize consumes only the exact projection returned by prepare.  All
-- recovery rows and hash-chain events are inserted before the persisted fence
-- can be switched back to normal; any failure rolls the transaction back.
CREATE OR REPLACE FUNCTION okg.external_action_restore_finalize(p_command JSONB)
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,okg
AS $external_action_restore_finalize_final$
DECLARE bound_deployment TEXT:=okg.external_action_session_deployment();
  session_row okg.external_action_restore_sessions%ROWTYPE;
  attestation_row okg.external_action_restore_attestations%ROWTYPE;
  existing_final okg.external_action_restore_finalize_idempotency%ROWTYPE;
  completion_row okg.external_action_restore_completion_receipts%ROWTYPE;
  fence_row okg.external_action_restore_fences%ROWTYPE;
  plan JSONB; projection JSONB; plan_projection JSONB; unsigned JSONB; coverage_command JSONB;
  recovery JSONB; event JSONB; event_projection JSONB; response JSONB;
  checkpoint_payload_for_recovery JSONB; outcome_to_record TEXT; outcome_reason_value TEXT; outcome_request_hash_value TEXT;
  operator_value TEXT; key_value TEXT; request_value TEXT; completed_value TIMESTAMPTZ;
  expected_request_hash TEXT;
BEGIN
  PERFORM okg.external_action_verifier_capability_status();
  PERFORM okg.external_action_require_command(p_command,
    ARRAY['deployment','restore_id','restore_attestation_envelope_digest','expected_inventory_digest',
      'expected_inventory_count','expected_checkpoint_set_digest','expected_checkpoint_count',
      'operator_separation_subject','idempotency_key_hash','request_hash','completion_receipt_envelope_digest',
      'completion_receipt_protected_b64','completion_receipt_payload_b64','completion_receipt_signature_b64',
      'completion_receipt_projection','completion_receipt_projection_digest','authentication_audit_id','nomos_decision_id'],
    ARRAY['deployment','restore_id','restore_attestation_envelope_digest','expected_inventory_digest',
      'expected_inventory_count','expected_checkpoint_set_digest','expected_checkpoint_count',
      'operator_separation_subject','idempotency_key_hash','request_hash','completion_receipt_envelope_digest',
      'completion_receipt_protected_b64','completion_receipt_payload_b64','completion_receipt_signature_b64',
      'completion_receipt_projection','completion_receipt_projection_digest','authentication_audit_id','nomos_decision_id']);
  IF p_command->>'deployment' IS DISTINCT FROM bound_deployment THEN
    RAISE EXCEPTION 'external action deployment is not bound to session_user' USING ERRCODE='28000';
  END IF;
  operator_value:=p_command->>'operator_separation_subject'; key_value:=p_command->>'idempotency_key_hash'; request_value:=p_command->>'request_hash';
  -- Resolve the immutable session and current authority before the terminal
  -- replay lookup.  A matching key cannot bypass a revoked audit/decision,
  -- while an exact replay remains independent of the current service mode.
  PERFORM pg_advisory_xact_lock(hashtextextended('okg.external_action.deployment:'||bound_deployment,0));
  SELECT * INTO session_row FROM okg.external_action_restore_sessions
   WHERE deployment_name=bound_deployment AND restore_id=p_command->>'restore_id' FOR SHARE;
  IF session_row.restore_session_row_id IS NULL OR session_row.operator_separation_subject IS DISTINCT FROM operator_value THEN
    RAISE EXCEPTION 'restore finalize operator is not session-bound' USING ERRCODE='42501';
  END IF;
  IF session_row.authentication_audit_id IS DISTINCT FROM p_command->>'authentication_audit_id'
     OR session_row.nomos_decision_id IS DISTINCT FROM p_command->>'nomos_decision_id' THEN
    RAISE EXCEPTION 'restore finalize authority is not session-bound' USING ERRCODE='42501';
  END IF;
  PERFORM okg.external_action_nomos_authority_snapshot(bound_deployment,p_command->>'authentication_audit_id',p_command->>'nomos_decision_id','external_action.restore_reconcile',p_command->>'request_hash');
  -- Resolve the terminal replay before any restore-only or expiry checks.
  SELECT * INTO existing_final FROM okg.external_action_restore_finalize_idempotency
   WHERE deployment_name=bound_deployment AND restore_id=p_command->>'restore_id' FOR SHARE;
  IF existing_final.finalize_idempotency_row_id IS NOT NULL THEN
    IF existing_final.operator_separation_subject IS DISTINCT FROM operator_value
       OR existing_final.idempotency_key_hash IS DISTINCT FROM key_value
       OR existing_final.request_hash IS DISTINCT FROM request_value THEN
      RAISE EXCEPTION 'restore finalize replay conflicts with the first operator, key, or request' USING ERRCODE='40001';
    END IF;
    RETURN convert_from(existing_final.response_body_jcs,'UTF8')::jsonb;
  END IF;
  expected_request_hash:=okg.external_action_hash(
    'okg-external-action-restore-finalize-request-v1',jsonb_build_object(
      'restore_attestation_envelope_digest',p_command->>'restore_attestation_envelope_digest',
      'expected_inventory_digest',p_command->>'expected_inventory_digest',
      'expected_inventory_count',(p_command->>'expected_inventory_count')::bigint,
      'expected_checkpoint_set_digest',p_command->>'expected_checkpoint_set_digest',
      'expected_checkpoint_count',(p_command->>'expected_checkpoint_count')::bigint));
  IF request_value IS DISTINCT FROM expected_request_hash THEN
    RAISE EXCEPTION 'restore finalize request hash is not canonical' USING ERRCODE='40001';
  END IF;
  SELECT * INTO session_row FROM okg.external_action_restore_sessions
   WHERE deployment_name=bound_deployment AND restore_id=p_command->>'restore_id' FOR UPDATE;
  SELECT * INTO completion_row FROM okg.external_action_restore_completion_receipts
   WHERE deployment_name=bound_deployment AND restore_id=session_row.restore_id FOR SHARE;
  IF completion_row.completion_receipt_row_id IS NOT NULL THEN
    RAISE EXCEPTION 'restore completion exists without its terminal idempotency record' USING ERRCODE='40001';
  END IF;
  SELECT * INTO attestation_row FROM okg.external_action_restore_attestations
   WHERE deployment_name=bound_deployment
     AND envelope_digest=session_row.restore_attestation_envelope_digest
   FOR SHARE;
  IF attestation_row.attestation_row_id IS NULL THEN
    RAISE EXCEPTION 'restore attestation projection is unavailable' USING ERRCODE='40001';
  END IF;
  projection:=p_command->'completion_receipt_projection';
  IF jsonb_typeof(projection) IS DISTINCT FROM 'object' THEN
    RAISE EXCEPTION 'completion receipt projection is not an object' USING ERRCODE='22023';
  END IF;
  completed_value:=NULLIF(projection->>'completed_at','')::timestamptz;
  IF completed_value IS NULL THEN
    RAISE EXCEPTION 'completion receipt has no completed_at' USING ERRCODE='22023';
  END IF;
  coverage_command:=jsonb_build_object('deployment',bound_deployment,'restore_id',session_row.restore_id,
    'restore_attestation_envelope_digest',p_command->>'restore_attestation_envelope_digest',
    'expected_inventory_digest',p_command->>'expected_inventory_digest','expected_inventory_count',p_command->>'expected_inventory_count',
    'expected_checkpoint_set_digest',p_command->>'expected_checkpoint_set_digest','expected_checkpoint_count',p_command->>'expected_checkpoint_count',
    'completed_at',to_char(completed_value AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'));
  plan:=okg.external_action_restore_build_plan(coverage_command);
  plan_projection:=plan->'projection';
  IF projection IS DISTINCT FROM plan_projection THEN
    RAISE EXCEPTION 'completion receipt projection is stale or does not match the immutable plan' USING ERRCODE='40001';
  END IF;
  IF okg.external_action_hash('okg-external-action-restore-completion-receipt-v1',projection) IS DISTINCT FROM p_command->>'completion_receipt_projection_digest' THEN
    RAISE EXCEPTION 'completion receipt projection digest is invalid' USING ERRCODE='22023';
  END IF;
  unsigned:=jsonb_build_object('protected_b64',p_command->>'completion_receipt_protected_b64','payload_b64',p_command->>'completion_receipt_payload_b64','signature_b64',p_command->>'completion_receipt_signature_b64');
  IF p_command->>'completion_receipt_envelope_digest' IS DISTINCT FROM okg.external_action_hash('okg-external-action-restore-completion-receipt-envelope-v1',unsigned) THEN
    RAISE EXCEPTION 'completion receipt envelope digest is invalid' USING ERRCODE='22023';
  END IF;
  IF p_command->>'completion_receipt_protected_b64' !~ '^[A-Za-z0-9_-]+$' OR p_command->>'completion_receipt_payload_b64' !~ '^[A-Za-z0-9_-]+$' OR p_command->>'completion_receipt_signature_b64' !~ '^[A-Za-z0-9_-]+$' THEN
    RAISE EXCEPTION 'completion receipt envelope encoding is invalid' USING ERRCODE='22023';
  END IF;
  FOR recovery IN SELECT value FROM jsonb_array_elements(plan->'recoveries') LOOP
    -- A recovered execution still needs the durable outcome row consumed by
    -- the normal projection.  The restore operator records only a database
    -- observation; this never performs provider I/O.
    IF recovery->>'source_execution_id' IS NOT NULL
       AND recovery->>'classification' IN ('database_restore_effect_uncertain','database_restore_succeeded','database_restore_failed')
       AND NOT EXISTS (
         SELECT 1 FROM okg.external_action_outcomes o
          WHERE o.deployment_name=bound_deployment AND o.execution_id=recovery->>'source_execution_id') THEN
      SELECT e.checkpoint_payload INTO checkpoint_payload_for_recovery
        FROM okg.external_action_restore_checkpoint_entries e
       WHERE e.deployment_name=bound_deployment AND e.restore_id=session_row.restore_id
         AND e.effect_instance_key=recovery->>'effect_instance_key';
      outcome_to_record:=CASE recovery->>'classification'
        WHEN 'database_restore_succeeded' THEN 'succeeded'
        WHEN 'database_restore_failed' THEN 'failed'
        ELSE COALESCE(NULLIF(checkpoint_payload_for_recovery->>'outcome',''),'indeterminate') END;
      -- A restore observation must never invent a reason outside the retained
      -- action contract. Prefer the phase-specific contract code when the
      -- contract carries it (provider_receipt_missing for an uncertain phase,
      -- provider_receipt_observed for a receipt, and
      -- provider_explicit_failure for an explicit failure); fall back to the
      -- lexicographically first valid code for deterministic legacy contracts.
      outcome_reason_value:=NULLIF(checkpoint_payload_for_recovery->>'outcome_reason_code','');
      IF outcome_reason_value IS NULL THEN
        SELECT COALESCE(
          min(r.reason_code) FILTER (WHERE r.reason_code = CASE outcome_to_record
            WHEN 'indeterminate' THEN 'provider_receipt_missing'
            WHEN 'succeeded' THEN 'provider_receipt_observed'
            WHEN 'failed' THEN 'provider_explicit_failure'
          END),
          min(r.reason_code))
          INTO outcome_reason_value
          FROM okg.external_action_proposals p
          CROSS JOIN LATERAL jsonb_each(p.action_contract_record->'outcome_reason_rules') AS r(reason_code,rule)
         WHERE p.deployment_name=bound_deployment
           AND p.proposal_id=recovery->>'source_proposal_id'
           AND (r.rule->'allowed_outcomes' ? outcome_to_record)
           AND r.rule->>'provider_receipt' = CASE
             WHEN checkpoint_payload_for_recovery->>'provider_receipt_digest' IS NULL
               THEN 'forbidden' ELSE 'required' END;
        IF outcome_reason_value IS NULL THEN
          RAISE EXCEPTION 'restore outcome has no retained action-contract reason'
            USING ERRCODE='40001',
              DETAIL=jsonb_build_object('code','restore_contract_reason_unavailable',
                'classification',recovery->>'classification',
                'effect_instance_key',recovery->>'effect_instance_key',
                'outcome',outcome_to_record)::text;
        END IF;
      END IF;
      IF outcome_reason_value IS NULL THEN
        RAISE EXCEPTION 'restore outcome has no retained action-contract reason'
          USING ERRCODE='40001',
            DETAIL=jsonb_build_object('classification',recovery->>'classification',
              'effect_instance_key',recovery->>'effect_instance_key')::text;
      END IF;
      outcome_request_hash_value:=okg.external_action_hash(
        'okg-external-action-outcome-request-v1',jsonb_build_object(
          'execution_id',recovery->>'source_execution_id',
          'effect_instance_key',recovery->>'effect_instance_key',
          'worker_or_reconciler_separation_subject',session_row.operator_separation_subject,
          'outcome',outcome_to_record,
          'outcome_reason_code',outcome_reason_value,
          'provider_receipt_digest',checkpoint_payload_for_recovery->>'provider_receipt_digest',
          'recovery_journal_digest',checkpoint_payload_for_recovery->>'journal_digest',
          'owner_unavailability_attestation_digest',NULL));
      INSERT INTO okg.external_action_outcomes
        (deployment_name,execution_id,effect_instance_key,worker_or_reconciler_separation_subject,
         outcome,outcome_reason_code,provider_receipt_digest,recovery_journal_digest,
         owner_unavailability_attestation_digest,request_hash,authentication_audit_id,
         nomos_decision_id,recorded_at,store_record)
      VALUES (bound_deployment,recovery->>'source_execution_id',recovery->>'effect_instance_key',
        session_row.operator_separation_subject,outcome_to_record,
        outcome_reason_value,
        checkpoint_payload_for_recovery->>'provider_receipt_digest',
        COALESCE(NULLIF(checkpoint_payload_for_recovery->>'journal_digest',''),recovery->>'recovery_digest'),
        NULL,outcome_request_hash_value,session_row.authentication_audit_id,
        session_row.nomos_decision_id,completed_value,recovery);
    END IF;
    INSERT INTO okg.external_action_restore_recoveries
      (deployment_name,old_epoch,new_epoch,effect_instance_key,submission_content_hash,checkpoint_digest,restore_attestation_digest,classification,source_proposal_id,source_decision_id,source_execution_id,recovered_review_state,recovered_execution_state,recovered_outcome,effective_at,recorded_at,pre_restore_effect_event_head_hash,restored_effect_event_head_hash,signed_worker_checkpoint_digest,recovery_digest,store_record)
    VALUES (bound_deployment,(SELECT old_epoch FROM okg.external_action_restore_attestations WHERE deployment_name=bound_deployment AND envelope_digest=session_row.restore_attestation_envelope_digest),
      (SELECT new_epoch FROM okg.external_action_restore_attestations WHERE deployment_name=bound_deployment AND envelope_digest=session_row.restore_attestation_envelope_digest),
      recovery->>'effect_instance_key',recovery->>'submission_content_hash',recovery->>'checkpoint_digest',session_row.restore_attestation_envelope_digest,recovery->>'classification',NULLIF(recovery->>'source_proposal_id',''),NULLIF(recovery->>'source_decision_id',''),NULLIF(recovery->>'source_execution_id',''),NULLIF(recovery->>'recovered_review_state',''),NULLIF(recovery->>'recovered_execution_state',''),NULLIF(recovery->>'recovered_outcome',''),(recovery->>'effective_at')::timestamptz,(recovery->>'recorded_at')::timestamptz,NULLIF(recovery->>'pre_restore_effect_event_head_hash',''),recovery->>'restored_effect_event_head_hash',recovery->>'checkpoint_digest',recovery->>'recovery_digest',recovery);
  END LOOP;
  FOR event IN SELECT value FROM jsonb_array_elements(plan->'events') LOOP
    event_projection:=event-'event_hash'-'projection_changed';
    INSERT INTO okg.external_action_events
      (deployment_name,proposal_id,execution_epoch,effect_instance_key,sequence,lifecycle_version,event_type,actor_kind,actor_principal,actor_separation_subject,authentication_audit_ref,nomos_decision_ref,deadline_kind,policy_digest,before_review_state,after_review_state,before_execution_state,after_execution_state,payload_version,payload_hash,decision_id,decision_binding_hash,execution_id,execution_binding_hash,transition_reason_code,outcome,outcome_reason_code,outcome_request_hash,restore_recovery_digest,effective_at,recorded_at,previous_event_hash,event_hash,store_record)
    VALUES (bound_deployment,NULLIF(event_projection->>'proposal_id',''),(event_projection->>'execution_epoch')::bigint,event_projection->>'effect_instance_key',(event_projection->>'sequence')::bigint,(event_projection->>'lifecycle_version')::bigint,event_projection->>'event_type',event_projection->>'actor_kind',event_projection->>'actor_principal',event_projection->>'actor_separation_subject',event_projection->>'authentication_audit_ref',event_projection->>'nomos_decision_ref',NULLIF(event_projection->>'deadline_kind',''),event_projection->>'policy_digest',NULLIF(event_projection->>'before_review_state',''),NULLIF(event_projection->>'after_review_state',''),NULLIF(event_projection->>'before_execution_state',''),NULLIF(event_projection->>'after_execution_state',''),NULLIF(event_projection->>'payload_version','')::bigint,event_projection->>'payload_hash',NULLIF(event_projection->>'decision_id',''),event_projection->>'decision_binding_hash',NULLIF(event_projection->>'execution_id',''),event_projection->>'execution_binding_hash',NULLIF(event_projection->>'transition_reason_code',''),event_projection->>'outcome',event_projection->>'outcome_reason_code',event_projection->>'outcome_request_hash',event_projection->>'restore_recovery_digest',(event_projection->>'effective_at')::timestamptz,(event_projection->>'recorded_at')::timestamptz,event_projection->>'previous_event_hash',event->>'event_hash',event_projection);
    IF event->>'projection_changed'='true' AND event->>'proposal_id' IS NOT NULL THEN
      UPDATE okg.external_action_proposals SET review_state=event->>'after_review_state',execution_state=event->>'after_execution_state',lifecycle_version=(event->>'lifecycle_version')::bigint,updated_at=completed_value
       WHERE deployment_name=bound_deployment AND proposal_id=event->>'proposal_id';
    END IF;
  END LOOP;
  response:=jsonb_build_object('replay',FALSE,'deployment',bound_deployment,'restore_id',session_row.restore_id,'state','complete','quarantine_state','cleared','service_mode','normal','recovery_count',(plan_projection->>'recovery_count')::bigint,'recovery_set_digest',plan_projection->>'recovery_set_digest','classification_summary',plan->'classification_summary','classifications',plan->'classification_summary','restore_completion_receipt',jsonb_build_object('envelope_digest',p_command->>'completion_receipt_envelope_digest'));
  INSERT INTO okg.external_action_restore_completion_receipts
    (deployment_name,restore_id,restore_attestation_envelope_digest,completion_receipt_envelope_digest,completed_at,authentication_audit_id,nomos_decision_id,store_record,protected_b64,payload_b64,signature_b64,completion_receipt_projection)
  VALUES (bound_deployment,session_row.restore_id,session_row.restore_attestation_envelope_digest,p_command->>'completion_receipt_envelope_digest',completed_value,p_command->>'authentication_audit_id',p_command->>'nomos_decision_id',response,p_command->>'completion_receipt_protected_b64',p_command->>'completion_receipt_payload_b64',p_command->>'completion_receipt_signature_b64',projection);
  INSERT INTO okg.external_action_restore_finalize_idempotency
    (deployment_name,restore_id,operator_separation_subject,idempotency_key_hash,request_hash,response_body_jcs,completion_receipt_envelope_digest)
  VALUES (bound_deployment,session_row.restore_id,operator_value,key_value,request_value,okg.external_action_json_bytes(response),p_command->>'completion_receipt_envelope_digest');
  SELECT * INTO fence_row FROM okg.external_action_restore_fences WHERE deployment_name=bound_deployment FOR UPDATE;
  IF fence_row.service_mode<>'restore_only' OR fence_row.quarantine_state<>'blocked' OR fence_row.highest_accepted_epoch>attestation_row.new_epoch THEN
    RAISE EXCEPTION 'restore fence changed while finalizing' USING ERRCODE='40001';
  END IF;
  UPDATE okg.external_action_restore_fences SET highest_accepted_epoch=attestation_row.new_epoch,lease_digest=attestation_row.new_epoch_lease_digest,quarantine_state='clear',service_mode='normal',restore_attestation_digest=attestation_row.envelope_digest,accepted_at=completed_value,mode_changed_at=completed_value WHERE deployment_name=bound_deployment;
  RETURN response;
END
$external_action_restore_finalize_final$;

-- Final ACL convergence for functions defined after the historical grants.
DO $external_action_restore_final_acl$
BEGIN
  REVOKE ALL ON FUNCTION okg.external_action_reject_mutation() FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_monotonic_nomos_updated_at() FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_nomos_authority_lock() FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_restore_create(JSONB) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_restore_batch(JSONB) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_restore_prepare(JSONB) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_restore_finalize(JSONB) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_restore_classification(JSONB,TEXT) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_restore_build_plan(JSONB) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_restore_inventory_digest(JSONB) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_restore_inventory(TEXT) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_restore_checkpoint_set_digest(TEXT) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_restore_event_root(TEXT) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_restore_assert_coverage(JSONB) FROM PUBLIC;
  REVOKE ALL ON FUNCTION okg.external_action_restore_lifecycle_gate() FROM PUBLIC;
  IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname='okg_external_action_api') THEN
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_create(JSONB) TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_batch(JSONB) TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_prepare(JSONB) TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_finalize(JSONB) TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_inventory(TEXT) TO okg_external_action_api;
  END IF;
  IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname='okg_external_action_function_owner') THEN
    ALTER FUNCTION okg.external_action_restore_create(JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_restore_batch(JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_restore_prepare(JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_restore_finalize(JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_restore_classification(JSONB,TEXT) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_restore_build_plan(JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_restore_inventory_digest(JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_restore_inventory(TEXT) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_restore_checkpoint_set_digest(TEXT) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_restore_event_root(TEXT) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_restore_assert_coverage(JSONB) OWNER TO okg_external_action_function_owner;
    ALTER FUNCTION okg.external_action_restore_lifecycle_gate() OWNER TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_create(JSONB) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_batch(JSONB) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_prepare(JSONB) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_finalize(JSONB) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_classification(JSONB,TEXT) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_build_plan(JSONB) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_inventory_digest(JSONB) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_inventory(TEXT) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_checkpoint_set_digest(TEXT) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_event_root(TEXT) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_assert_coverage(JSONB) TO okg_external_action_function_owner;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_lifecycle_gate() TO okg_external_action_function_owner;
  END IF;
  IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname='okg_external_action_api') THEN
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_create(JSONB) TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_batch(JSONB) TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_prepare(JSONB) TO okg_external_action_api;
    GRANT EXECUTE ON FUNCTION okg.external_action_restore_finalize(JSONB) TO okg_external_action_api;
  END IF;
END
$external_action_restore_final_acl$;

-- Reassert API privileges after the final owner transfer and before the last
-- helper replacement.  These are the only restore functions exposed to the
-- API role; all planning/validation helpers remain function-owner only.
GRANT EXECUTE ON FUNCTION okg.external_action_restore_create(JSONB) TO okg_external_action_api;
GRANT EXECUTE ON FUNCTION okg.external_action_restore_batch(JSONB) TO okg_external_action_api;
GRANT EXECUTE ON FUNCTION okg.external_action_restore_prepare(JSONB) TO okg_external_action_api;
GRANT EXECUTE ON FUNCTION okg.external_action_restore_finalize(JSONB) TO okg_external_action_api;
GRANT EXECUTE ON FUNCTION okg.external_action_restore_inventory(TEXT) TO okg_external_action_api;

CREATE OR REPLACE FUNCTION okg.external_action_restore_assert_coverage(p_command JSONB)
RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $external_action_restore_assert_coverage$
DECLARE bound_deployment TEXT:=okg.external_action_session_deployment();
  session_row okg.external_action_restore_sessions%ROWTYPE;
  attestation_row okg.external_action_restore_attestations%ROWTYPE;
  fence_row okg.external_action_restore_fences%ROWTYPE;
  keys JSONB; inventory JSONB; db_inventory JSONB; derived_inventory JSONB; checkpoint_set JSONB; count_value BIGINT;
  inventory_count BIGINT; derived_inventory_count BIGINT; derived_inventory_digest TEXT;
  expected_inventory_digest TEXT; expected_checkpoint_digest TEXT; pre_root JSONB;
  positive_count BIGINT; positive_distinct_count BIGINT; positive_min BIGINT; positive_max BIGINT; positive_previous TEXT; last_positive_key TEXT;
BEGIN
  PERFORM okg.external_action_require_command(
    p_command,
    ARRAY['deployment','restore_id','restore_attestation_envelope_digest',
      'expected_inventory_digest','expected_inventory_count',
      'expected_checkpoint_set_digest','expected_checkpoint_count'],
    ARRAY['deployment','restore_id','restore_attestation_envelope_digest',
      'expected_inventory_digest','expected_inventory_count',
      'expected_checkpoint_set_digest','expected_checkpoint_count']
  );
  IF p_command->>'deployment' IS DISTINCT FROM bound_deployment THEN
    RAISE EXCEPTION 'external action deployment is not bound to session_user' USING ERRCODE='28000';
  END IF;
  SELECT * INTO session_row FROM okg.external_action_restore_sessions
   WHERE deployment_name=bound_deployment AND restore_id=p_command->>'restore_id' FOR UPDATE;
  IF session_row.restore_session_row_id IS NULL THEN
    RAISE EXCEPTION 'restore session is not finalizable' USING ERRCODE='40001';
  END IF;
  SELECT * INTO fence_row FROM okg.external_action_restore_fences
   WHERE deployment_name=bound_deployment FOR UPDATE;
  IF fence_row.deployment_name IS NULL OR fence_row.service_mode<>'restore_only'
     OR fence_row.quarantine_state<>'blocked' THEN
    RAISE EXCEPTION 'restore fence is not blocked in restore_only mode' USING ERRCODE='40001';
  END IF;
  IF session_row.restore_attestation_envelope_digest IS DISTINCT FROM
     p_command->>'restore_attestation_envelope_digest' THEN
    RAISE EXCEPTION 'restore attestation does not match session' USING ERRCODE='40001';
  END IF;
  SELECT * INTO attestation_row FROM okg.external_action_restore_attestations
   WHERE deployment_name=bound_deployment AND envelope_digest=session_row.restore_attestation_envelope_digest
   FOR SHARE;
  IF attestation_row.attestation_row_id IS NULL THEN
    RAISE EXCEPTION 'restore attestation projection is unavailable' USING ERRCODE='40001';
  END IF;
  pre_root:=okg.external_action_restore_event_root(bound_deployment);
  IF pre_root->>'digest' IS DISTINCT FROM attestation_row.pre_restore_event_root_digest
     OR (pre_root->>'count')::bigint IS DISTINCT FROM attestation_row.pre_restore_event_head_count THEN
    RAISE EXCEPTION 'restore pre-event root or count is stale' USING ERRCODE='40001';
  END IF;
  IF (p_command->>'expected_inventory_count')::bigint<>attestation_row.effect_inventory_count
     OR p_command->>'expected_inventory_digest'<>attestation_row.effect_inventory_digest
     OR (p_command->>'expected_checkpoint_count')::bigint<>attestation_row.worker_checkpoint_count
     OR p_command->>'expected_checkpoint_set_digest'<>attestation_row.worker_checkpoint_set_digest THEN
    RAISE EXCEPTION 'restore attestation root or count disagrees with finalize request' USING ERRCODE='40001';
  END IF;
  inventory:=COALESCE(session_row.effect_inventory,'[]'::jsonb);
  inventory_count:=jsonb_array_length(inventory);
  expected_inventory_digest:=okg.external_action_restore_inventory_digest(inventory);
  IF inventory_count<>session_row.effect_inventory_count
     OR session_row.effect_inventory_digest IS DISTINCT FROM expected_inventory_digest THEN
    RAISE EXCEPTION 'restore database inventory projection is not canonical' USING ERRCODE='40001';
  END IF;
  SELECT count(*)::bigint,
         COALESCE(jsonb_agg(to_jsonb(effect_instance_key)
           ORDER BY convert_to(effect_instance_key,'UTF8')),'[]'::jsonb)
    INTO count_value,keys
    FROM okg.external_action_restore_checkpoint_entries
   WHERE deployment_name=bound_deployment AND restore_id=session_row.restore_id;
  IF count_value<>attestation_row.worker_checkpoint_count THEN
    RAISE EXCEPTION 'restore checkpoint coverage is incomplete or has an extra effect key'
      USING ERRCODE='40001',
        DETAIL=jsonb_build_object('code','incomplete_restore_coverage',
          'quarantine_state','blocked',
          'expected_checkpoint_count',attestation_row.worker_checkpoint_count,
          'accepted_checkpoint_count',count_value)::text;
  END IF;
  checkpoint_set:=okg.external_action_restore_checkpoint_set_digest(session_row.restore_id);
  expected_checkpoint_digest:=checkpoint_set->>'digest';
  IF checkpoint_set->>'count'<>(p_command->>'expected_checkpoint_count')
     OR expected_checkpoint_digest<>p_command->>'expected_checkpoint_set_digest' THEN
    RAISE EXCEPTION 'restore checkpoint set digest or count is not exact' USING ERRCODE='40001';
  END IF;
  IF EXISTS (
    SELECT 1 FROM okg.external_action_restore_checkpoint_entries e
     WHERE e.deployment_name=bound_deployment AND e.restore_id=session_row.restore_id
       AND (jsonb_typeof(e.checkpoint_payload) IS DISTINCT FROM 'object'
            OR e.checkpoint_payload->>'proof_kind' NOT IN ('positive','nonmembership'))
  ) THEN
    RAISE EXCEPTION 'restore checkpoint lacks a verified recovery projection' USING ERRCODE='40001';
  END IF;
  IF EXISTS (
    SELECT 1 FROM okg.external_action_restore_checkpoint_entries e
     WHERE e.deployment_name=bound_deployment AND e.restore_id=session_row.restore_id
       AND (
         e.checkpoint_payload->>'deployment' IS DISTINCT FROM bound_deployment
         OR e.checkpoint_payload->>'effect_instance_key' IS DISTINCT FROM e.effect_instance_key
         OR e.checkpoint_payload->>'execution_epoch' IS DISTINCT FROM attestation_row.old_epoch::text
         OR e.checkpoint_payload->>'ledger_fence_envelope_digest' IS DISTINCT FROM attestation_row.ledger_fence_envelope_digest
         OR e.checkpoint_payload->>'fenced_ledger_root_digest' IS DISTINCT FROM attestation_row.fenced_ledger_root_digest
         OR e.checkpoint_payload->>'fenced_ledger_entry_count' IS DISTINCT FROM attestation_row.fenced_ledger_entry_count::text
         OR e.checkpoint_payload->>'checkpoint_issued_at' IS NULL
         OR e.checkpoint_payload->>'submission_content_hash' IS NULL
         OR (e.checkpoint_payload->>'proof_kind'='positive'
             AND e.checkpoint_payload->>'positive_proof_digest' IS DISTINCT FROM e.proof_digest)
         OR (e.checkpoint_payload->>'proof_kind'='nonmembership'
             AND e.checkpoint_payload->>'nonmembership_proof_digest' IS DISTINCT FROM e.proof_digest)
       )
  ) THEN
    RAISE EXCEPTION 'restore checkpoint is not bound to the attested fence projection' USING ERRCODE='40001';
  END IF;
  IF EXISTS (
    SELECT 1 FROM okg.external_action_restore_checkpoint_entries e
     WHERE e.deployment_name=bound_deployment AND e.restore_id=session_row.restore_id
       AND e.checkpoint_payload->>'proof_kind'='positive'
       AND (e.leaf_index IS NULL OR e.leaf_index < 0)
  ) THEN
    RAISE EXCEPTION 'positive checkpoint index is not canonical decimal' USING ERRCODE='40001';
  END IF;
  SELECT count(*) FILTER (WHERE e.checkpoint_payload->>'proof_kind'='positive'),
         count(DISTINCT e.leaf_index) FILTER (WHERE e.checkpoint_payload->>'proof_kind'='positive'),
         min(e.leaf_index) FILTER (WHERE e.checkpoint_payload->>'proof_kind'='positive'),
         max(e.leaf_index) FILTER (WHERE e.checkpoint_payload->>'proof_kind'='positive')
    INTO positive_count,positive_distinct_count,positive_min,positive_max
    FROM okg.external_action_restore_checkpoint_entries e
   WHERE e.deployment_name=bound_deployment AND e.restore_id=session_row.restore_id;
  IF positive_count<>attestation_row.fenced_ledger_entry_count
     OR positive_distinct_count<>positive_count
     OR (positive_count>0 AND (positive_min<>0 OR positive_max<>positive_count-1)) THEN
    RAISE EXCEPTION 'positive checkpoint indexes do not cover the fenced ledger exactly' USING ERRCODE='40001';
  END IF;
  last_positive_key:=NULL;
  FOR positive_previous IN SELECT e.effect_instance_key FROM okg.external_action_restore_checkpoint_entries e
    WHERE e.deployment_name=bound_deployment AND e.restore_id=session_row.restore_id
      AND e.checkpoint_payload->>'proof_kind'='positive'
    ORDER BY e.leaf_index LOOP
    IF positive_previous IS NULL OR btrim(positive_previous)='' THEN
      RAISE EXCEPTION 'positive checkpoint has no effect key' USING ERRCODE='40001';
    END IF;
    IF last_positive_key IS NOT NULL AND convert_to(positive_previous,'UTF8')<=convert_to(last_positive_key,'UTF8') THEN
      RAISE EXCEPTION 'positive checkpoint ledger keys are not strictly sorted' USING ERRCODE='40001';
    END IF;
    last_positive_key:=positive_previous;
  END LOOP;
  SELECT COALESCE(jsonb_agg(to_jsonb(effect_instance_key)
      ORDER BY convert_to(effect_instance_key,'UTF8')),'[]'::jsonb)
    INTO db_inventory
    FROM (
      SELECT effect_instance_key FROM okg.external_action_proposals
       WHERE deployment_name=bound_deployment AND execution_epoch=attestation_row.old_epoch
      UNION
      SELECT d.effect_instance_key FROM okg.external_action_decisions d
       JOIN okg.external_action_proposals p ON p.deployment_name=d.deployment_name AND p.proposal_id=d.proposal_id
       WHERE d.deployment_name=bound_deployment AND p.execution_epoch=attestation_row.old_epoch
      UNION
      SELECT effect_instance_key FROM okg.external_action_executions
       WHERE deployment_name=bound_deployment AND execution_epoch=attestation_row.old_epoch
      UNION
      SELECT o.effect_instance_key FROM okg.external_action_outcomes o
       JOIN okg.external_action_executions e ON e.deployment_name=o.deployment_name AND e.execution_id=o.execution_id
       WHERE o.deployment_name=bound_deployment AND e.execution_epoch=attestation_row.old_epoch
      UNION
      SELECT effect_instance_key FROM okg.external_action_events
       WHERE deployment_name=bound_deployment AND execution_epoch=attestation_row.old_epoch
    ) database_inventory;
  IF db_inventory IS DISTINCT FROM inventory THEN
    RAISE EXCEPTION 'restore database inventory projection is not the retained old-epoch set' USING ERRCODE='40001';
  END IF;
  SELECT COALESCE(jsonb_agg(to_jsonb(effect_instance_key)
      ORDER BY convert_to(effect_instance_key,'UTF8')),'[]'::jsonb)
    INTO derived_inventory
    FROM (
      SELECT value #>> '{}' AS effect_instance_key
        FROM jsonb_array_elements(db_inventory) AS database_item(value)
      UNION
      SELECT e.effect_instance_key FROM okg.external_action_restore_checkpoint_entries e
       WHERE e.deployment_name=bound_deployment AND e.restore_id=session_row.restore_id
         AND e.checkpoint_payload->>'proof_kind'='positive'
    ) inventory_union;
  derived_inventory_count:=jsonb_array_length(derived_inventory);
  derived_inventory_digest:=okg.external_action_restore_inventory_digest(derived_inventory);
  IF derived_inventory_count<>attestation_row.effect_inventory_count
     OR derived_inventory_digest<>attestation_row.effect_inventory_digest
     OR count_value<>derived_inventory_count
     OR keys IS DISTINCT FROM derived_inventory THEN
    RAISE EXCEPTION 'restore inventory is not the database plus positive-ledger union' USING ERRCODE='40001';
  END IF;
  RETURN jsonb_build_object(
    'deployment',bound_deployment,'restore_id',session_row.restore_id,
    'restore_attestation_envelope_digest',attestation_row.envelope_digest,
    'effect_inventory_digest',attestation_row.effect_inventory_digest,
    'effect_inventory_count',attestation_row.effect_inventory_count,
    'worker_checkpoint_set_digest',attestation_row.worker_checkpoint_set_digest,
    'worker_checkpoint_count',attestation_row.worker_checkpoint_count,
    'pre_restore_event_root_digest',attestation_row.pre_restore_event_root_digest,
    'pre_restore_event_head_count',attestation_row.pre_restore_event_head_count
  );
END
$external_action_restore_assert_coverage$;
