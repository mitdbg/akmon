-- Record a late claim's execution-deadline expiry (issue #1925).
--
-- okg.external_action_prepare_claim and okg.external_action_claim used to
-- RAISE SQLSTATE 22008 when execute_before had passed. The raise rolled back
-- the caller's transaction, so no approval_expired event was written, and the
-- store reported the refusal as a generic unavailable authority. Both now
-- call okg.external_action_materialize_expiry and RETURN a closed
-- {refusal: proposal_expired, proposal_id} result, so the event commits.
--
-- Why a migration and not only schema.sql: migrate executes schema.sql first
-- and then every pending ordered migration. A database that has not yet
-- applied 0030 would get these two functions back from 0030's older bodies
-- after schema.sql ran; this file, which runs after 0030, restores the
-- current bodies. It is the same pattern 0033 uses for
-- okg.external_action_store_snapshot. The bodies below are byte-identical to
-- schema.sql. CREATE OR REPLACE keeps each function's owner and grants.
-- It does not keep SECURITY DEFINER or the search_path setting; they hold
-- because each definition below restates them. Idempotent; no relation or
-- row is touched.
SET lock_timeout = '5s';
SET statement_timeout = '120s';

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
  expiry_value JSONB;
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
  -- An expired proposal is refused with a closed result, not an exception,
  -- so the caller can tell a passed deadline from an unavailable database.
  IF proposal_row.review_state = 'expired' THEN
    RETURN jsonb_build_object('refusal','proposal_expired',
      'proposal_id',proposal_row.proposal_id);
  END IF;
  IF proposal_row.review_state <> 'approved'
     OR proposal_row.execution_state <> 'ready' THEN
    RAISE EXCEPTION 'external action proposal is not ready to claim'
      USING ERRCODE='40001';
  END IF;
  observed_at := clock_timestamp();
  IF proposal_row.execute_before IS NULL OR observed_at >= proposal_row.execute_before THEN
    -- Record the elapsed execution deadline and RETURN the refusal: a raise
    -- here would roll back the approval_expired event with the transaction
    -- (issue #1925).  A NULL deadline cannot be materialized and stays the
    -- generic refusal.
    IF proposal_row.execute_before IS NOT NULL THEN
      expiry_value := okg.external_action_materialize_expiry(proposal_row.proposal_id);
      IF (expiry_value ->> 'expired')::boolean IS TRUE THEN
        RETURN jsonb_build_object('refusal','proposal_expired',
          'proposal_id',proposal_row.proposal_id);
      END IF;
    END IF;
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
  expiry_value JSONB;
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
  -- An expired proposal is refused with a closed result, not an exception,
  -- so the caller can tell a passed deadline from an unavailable database.
  IF proposal_row.review_state='expired' THEN
    RETURN jsonb_build_object('refusal','proposal_expired','proposal_id',proposal_row.proposal_id);
  END IF;
  IF proposal_row.review_state<>'approved' OR proposal_row.execution_state<>'ready' THEN RAISE EXCEPTION 'external action proposal is not ready to claim' USING ERRCODE='40001'; END IF;
  IF proposal_row.execute_before IS NULL OR clock_timestamp()>=proposal_row.execute_before OR claimed_at_value>=proposal_row.execute_before THEN
    -- Record the elapsed execution deadline and RETURN the refusal: a raise
    -- here would roll back the approval_expired event with the transaction
    -- (issue #1925).  A NULL deadline, or a prepared claimed_at past a
    -- deadline the database clock has not reached, stays the generic refusal.
    IF proposal_row.execute_before IS NOT NULL AND clock_timestamp()>=proposal_row.execute_before THEN
      expiry_value:=okg.external_action_materialize_expiry(proposal_row.proposal_id);
      IF (expiry_value->>'expired')::boolean IS TRUE THEN
        RETURN jsonb_build_object('refusal','proposal_expired','proposal_id',proposal_row.proposal_id);
      END IF;
    END IF;
    RAISE EXCEPTION 'external action execution deadline has passed' USING ERRCODE='22008';
  END IF;
  IF proposal_row.current_decision_id<>p_command->>'decision_id' OR proposal_row.current_decision_binding_hash IS NULL THEN RAISE EXCEPTION 'external action approval binding is stale' USING ERRCODE='40001'; END IF;
  SELECT * INTO decision_row FROM okg.external_action_decisions WHERE deployment_name=bound_deployment AND decision_id=p_command->>'decision_id' AND proposal_id=proposal_row.proposal_id FOR SHARE;
  IF decision_row.decision_id IS NULL OR decision_row.verdict<>'approved' OR decision_row.decision_binding_hash<>proposal_row.current_decision_binding_hash THEN RAISE EXCEPTION 'external action approval decision is unavailable' USING ERRCODE='40001'; END IF;
  IF p_command->>'policy_digest' IS DISTINCT FROM decision_row.policy_digest THEN RAISE EXCEPTION 'external action policy binding is stale' USING ERRCODE='40001'; END IF;
  IF (p_command->>'approved_lifecycle_version')::bigint<>proposal_row.lifecycle_version OR (p_command->>'approved_payload_version')::bigint<>proposal_row.payload_version OR p_command->>'approved_payload_hash'<>proposal_row.current_payload_hash THEN RAISE EXCEPTION 'external action approval payload binding is stale' USING ERRCODE='40001'; END IF;
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
