-- Immutable registration facts; no execution or budget authority.
SET lock_timeout = '5s';
SET statement_timeout = '120s';

CREATE TABLE IF NOT EXISTS okg.external_automation_runs (
  record_id TEXT PRIMARY KEY,
  deployment_name TEXT NOT NULL,
  owner_separation_subject TEXT NOT NULL,
  run_id TEXT NOT NULL,
  automation_id TEXT NOT NULL,
  automation_version TEXT NOT NULL,
  correlation_id TEXT NOT NULL,
  reported_distribution JSONB NOT NULL,
  generation_id TEXT COLLATE "C" NOT NULL,
  registration_idempotency_key_hash TEXT NOT NULL,
  request_hash TEXT NOT NULL,
  authentication_audit_id TEXT NOT NULL,
  nomos_decision_id TEXT NOT NULL,
  registered_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  UNIQUE (deployment_name, record_id),
  UNIQUE (deployment_name, owner_separation_subject, run_id),
  UNIQUE (deployment_name, owner_separation_subject, registration_idempotency_key_hash)
);
CREATE TABLE IF NOT EXISTS okg.external_automation_run_proposals (
  deployment_name TEXT NOT NULL,
  record_id TEXT NOT NULL,
  proposal_id TEXT NOT NULL,
  owner_separation_subject TEXT NOT NULL,
  link_idempotency_key_hash TEXT NOT NULL,
  request_hash TEXT NOT NULL,
  authentication_audit_id TEXT NOT NULL,
  nomos_decision_id TEXT NOT NULL,
  proposal_read_authentication_audit_id TEXT NOT NULL,
  proposal_read_nomos_decision_id TEXT NOT NULL,
  linked_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (deployment_name, proposal_id),
  UNIQUE (record_id, owner_separation_subject, link_idempotency_key_hash),
  FOREIGN KEY (deployment_name, record_id)
    REFERENCES okg.external_automation_runs(deployment_name, record_id),
  FOREIGN KEY (deployment_name, proposal_id)
    REFERENCES okg.external_action_proposals(deployment_name, proposal_id)
);
ALTER TABLE okg.external_automation_runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE okg.external_automation_run_proposals ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS external_automation_runs_deployment ON okg.external_automation_runs;
CREATE POLICY external_automation_runs_deployment ON okg.external_automation_runs
  TO okg_external_action_function_owner
  USING (deployment_name = okg.external_action_session_deployment())
  WITH CHECK (deployment_name = okg.external_action_session_deployment());
DROP POLICY IF EXISTS external_automation_run_proposals_deployment ON okg.external_automation_run_proposals;
CREATE POLICY external_automation_run_proposals_deployment ON okg.external_automation_run_proposals
  TO okg_external_action_function_owner
  USING (deployment_name = okg.external_action_session_deployment())
  WITH CHECK (deployment_name = okg.external_action_session_deployment());

CREATE OR REPLACE FUNCTION okg.external_automation_transaction_context()
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, okg AS $run_tx$
BEGIN
  PERFORM okg.external_action_verifier_capability_status();
  PERFORM okg.external_action_session_deployment();
  RETURN jsonb_build_object('transaction_id', pg_current_xact_id()::text);
END $run_tx$;

-- Internal helper: no API-role EXECUTE grant. Every semantic RPC calls it.
CREATE OR REPLACE FUNCTION okg.external_automation_authority(
  p_command JSONB, p_operation TEXT, p_prefix TEXT
) RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, okg AS $run_authority$
DECLARE
  dep TEXT := okg.external_action_session_deployment();
  target_value JSONB;
  digest_value TEXT;
  snapshot JSONB;
  decision_target JSONB;
  decision_resource JSONB;
  proposal_value TEXT := p_command->'body'->>'proposal_id';
BEGIN
  PERFORM okg.external_action_verifier_capability_status();
  IF p_command->>'deployment' IS DISTINCT FROM dep
     OR p_command->>'owner' IS NULL OR p_command->>'record_id' IS NULL
     OR p_command->>'request_hash' IS NULL THEN
    RAISE EXCEPTION 'run_authority_invalid' USING ERRCODE = '42501';
  END IF;
  target_value := jsonb_build_object(
    'kind', CASE WHEN p_operation = 'external_action.read' THEN 'external_action' ELSE 'external_automation_run' END,
    'id', CASE WHEN p_operation = 'external_action.read' THEN proposal_value ELSE p_command->>'record_id' END,
    'operation', p_operation
  );
  digest_value := okg.external_action_hash('okg-external-automation-authorization-v1', jsonb_build_object(
    'transaction_id', pg_current_xact_id()::text, 'deployment', dep,
    'owner', p_command->>'owner', 'record_id', p_command->>'record_id',
    'proposal_id', proposal_value, 'request_hash', p_command->>'request_hash',
    'operation', p_operation, 'target', target_value
  ));
  snapshot := okg.external_action_nomos_authority_snapshot(
    dep, p_command->>(p_prefix || '_authentication_audit_id'),
    p_command->>(p_prefix || '_nomos_decision_id'), p_operation, digest_value
  );
  SELECT target, resource INTO decision_target, decision_resource FROM okg.nomos_decisions
    WHERE deployment_name = dep AND decision_id = p_command->>(p_prefix || '_nomos_decision_id');
  IF snapshot->>'separation_subject' IS DISTINCT FROM p_command->>'owner'
     OR decision_target IS DISTINCT FROM target_value
     OR decision_resource IS DISTINCT FROM (target_value || jsonb_build_object('resource_kind', target_value->>'kind', 'nomos_surface', p_operation, 'deployment', dep, 'scope', 'external_action')) THEN
    RAISE EXCEPTION 'run_authority_invalid' USING ERRCODE = '42501';
  END IF;
  RETURN snapshot;
END $run_authority$;

CREATE OR REPLACE FUNCTION okg.external_automation_projection(p_record_id TEXT)
RETURNS JSONB LANGUAGE sql SECURITY DEFINER
SET search_path = pg_catalog, okg AS $run_projection$
  SELECT jsonb_build_object(
    'record_id', r.record_id, 'deployment', r.deployment_name,
    'owner', r.owner_separation_subject, 'run_id', r.run_id,
    'automation_id', r.automation_id, 'automation_version', r.automation_version,
    'correlation_id', r.correlation_id,
    'reported_distribution', r.reported_distribution || '{"verification":"caller_reported"}'::jsonb,
    'generation', jsonb_build_object('generation_id', r.generation_id, 'verification', 'reference_verified_at_registration'),
    'registered_at', r.registered_at,
    'proposals', COALESCE((SELECT jsonb_agg(jsonb_build_object('proposal_id', l.proposal_id, 'linked_at', l.linked_at) ORDER BY l.proposal_id)
      FROM okg.external_automation_run_proposals l WHERE l.deployment_name = r.deployment_name AND l.record_id = r.record_id), '[]'::jsonb)
  ) FROM okg.external_automation_runs r
  WHERE r.record_id = p_record_id AND r.deployment_name = okg.external_action_session_deployment()
$run_projection$;

CREATE OR REPLACE FUNCTION okg.external_automation_register(p_command JSONB)
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, okg AS $run_register$
DECLARE
  dep TEXT := okg.external_action_session_deployment();
  b JSONB := p_command->'body';
  distribution JSONB := b->'reported_distribution';
  prior okg.external_automation_runs%ROWTYPE;
  field_name TEXT;
  key_hash TEXT := p_command->>'idempotency_key_hash';
  expected_hash TEXT;
BEGIN
  PERFORM okg.external_action_require_command(p_command,
    ARRAY['deployment','owner','record_id','request_hash','idempotency_key_hash','body','primary_authentication_audit_id','primary_nomos_decision_id'],
    ARRAY['deployment','owner','record_id','request_hash','idempotency_key_hash','body','primary_authentication_audit_id','primary_nomos_decision_id']);
  PERFORM okg.external_automation_authority(p_command, 'external_automation.register', 'primary');
  PERFORM okg.external_action_require_command(b,
    ARRAY['run_id','automation_id','automation_version','correlation_id','reported_distribution','generation_id'],
    ARRAY['run_id','automation_id','automation_version','correlation_id','reported_distribution','generation_id']);
  PERFORM okg.external_action_require_command(distribution,
    ARRAY['name','version','package_digest'], ARRAY['name','version']);
  IF NOT (distribution ? 'package_digest') THEN RAISE EXCEPTION 'invalid_run_record' USING ERRCODE = '22023'; END IF;
  FOREACH field_name IN ARRAY ARRAY['run_id','automation_id','automation_version','correlation_id'] LOOP
    IF jsonb_typeof(b->field_name) IS DISTINCT FROM 'string'
       OR b->>field_name !~ '^[A-Za-z0-9._:+-]{1,128}$'
       OR (field_name = 'automation_version' AND length(b->>field_name) > 64) THEN
      RAISE EXCEPTION 'invalid_run_record' USING ERRCODE = '22023';
    END IF;
  END LOOP;
  IF jsonb_typeof(distribution->'name') IS DISTINCT FROM 'string'
     OR jsonb_typeof(distribution->'version') IS DISTINCT FROM 'string'
     OR distribution->>'name' !~ '^[A-Za-z0-9._:+-]{1,128}$'
     OR distribution->>'version' !~ '^[A-Za-z0-9._:+-]{1,128}$'
     OR (distribution->'package_digest' <> 'null'::jsonb AND (jsonb_typeof(distribution->'package_digest') <> 'string' OR distribution->>'package_digest' !~ '^sha256:[0-9a-f]{64}$'))
     OR jsonb_typeof(b->'generation_id') IS DISTINCT FROM 'string'
     OR octet_length(b->>'generation_id') NOT BETWEEN 1 AND 1024
     OR b->>'generation_id' ~ '[[:cntrl:]]'
     OR p_command->>'record_id' !~ '^[0-9A-HJKMNP-TV-Z]{26}$'
     OR key_hash !~ '^sha256:[0-9a-f]{64}$' THEN
    RAISE EXCEPTION 'invalid_run_record' USING ERRCODE = '22023';
  END IF;
  expected_hash := okg.external_action_hash('okg-external-automation-request-v1', jsonb_build_object(
    'operation','register','deployment',dep,'owner',p_command->>'owner','record_id',NULL,'body',b));
  IF expected_hash IS DISTINCT FROM p_command->>'request_hash' THEN
    RAISE EXCEPTION 'run_authority_invalid' USING ERRCODE = '42501';
  END IF;
  -- Authority snapshot holds the deployment advisory lock, serializing keys.
  SELECT * INTO prior FROM okg.external_automation_runs
    WHERE deployment_name = dep AND owner_separation_subject = p_command->>'owner'
      AND registration_idempotency_key_hash = key_hash;
  IF FOUND THEN
    IF prior.request_hash <> expected_hash THEN
      RAISE EXCEPTION 'run_identity_conflict' USING ERRCODE = '40001';
    END IF;
    RETURN jsonb_build_object('status',200,'body',okg.external_automation_projection(prior.record_id));
  END IF;
  IF NOT EXISTS (SELECT 1 FROM okg.external_action_restore_fences WHERE deployment_name = dep AND service_mode = 'normal') THEN
    RAISE EXCEPTION 'run_restore_quarantined' USING ERRCODE = '55000';
  END IF;
  IF EXISTS (SELECT 1 FROM okg.external_automation_runs WHERE deployment_name = dep AND owner_separation_subject = p_command->>'owner' AND run_id = b->>'run_id') THEN
    RAISE EXCEPTION 'run_identity_conflict' USING ERRCODE = '40001';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM okg.v_generations WHERE id COLLATE "C" = (b->>'generation_id') COLLATE "C" AND deployment_name = dep) THEN
    RAISE EXCEPTION 'run_generation_invalid' USING ERRCODE = '22023';
  END IF;
  INSERT INTO okg.external_automation_runs(record_id,deployment_name,owner_separation_subject,run_id,automation_id,automation_version,correlation_id,reported_distribution,generation_id,registration_idempotency_key_hash,request_hash,authentication_audit_id,nomos_decision_id)
  VALUES(p_command->>'record_id',dep,p_command->>'owner',b->>'run_id',b->>'automation_id',b->>'automation_version',b->>'correlation_id',distribution,b->>'generation_id',key_hash,expected_hash,p_command->>'primary_authentication_audit_id',p_command->>'primary_nomos_decision_id');
  RETURN jsonb_build_object('status',201,'body',okg.external_automation_projection(p_command->>'record_id'));
END $run_register$;

CREATE OR REPLACE FUNCTION okg.external_automation_read(p_command JSONB)
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, okg AS $run_read$
DECLARE dep TEXT := okg.external_action_session_deployment();
BEGIN
  PERFORM okg.external_action_require_command(p_command,
    ARRAY['deployment','owner','record_id','request_hash','idempotency_key_hash','body','primary_authentication_audit_id','primary_nomos_decision_id'],
    ARRAY['deployment','owner','record_id','request_hash','body','primary_authentication_audit_id','primary_nomos_decision_id']);
  PERFORM okg.external_automation_authority(p_command, 'external_automation.read', 'primary');
  IF p_command->'body' IS DISTINCT FROM '{}'::jsonb OR p_command->>'request_hash' IS DISTINCT FROM
    okg.external_action_hash('okg-external-automation-request-v1', jsonb_build_object('operation','read','deployment',dep,'owner',p_command->>'owner','record_id',p_command->>'record_id','body','{}'::jsonb)) THEN
    RAISE EXCEPTION 'invalid_run_record' USING ERRCODE = '22023';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM okg.external_automation_runs WHERE deployment_name = dep AND record_id = p_command->>'record_id' AND owner_separation_subject = p_command->>'owner') THEN
    RAISE EXCEPTION 'run_not_found' USING ERRCODE = 'P0002';
  END IF;
  RETURN jsonb_build_object('status',200,'body',okg.external_automation_projection(p_command->>'record_id'));
END $run_read$;

CREATE OR REPLACE FUNCTION okg.external_automation_link(p_command JSONB)
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, okg AS $run_link$
DECLARE
  dep TEXT := okg.external_action_session_deployment();
  first_auth JSONB; second_auth JSONB;
  r okg.external_automation_runs%ROWTYPE;
  p okg.external_action_proposals%ROWTYPE;
  prior okg.external_automation_run_proposals%ROWTYPE;
  expected_hash TEXT;
  field_name TEXT;
BEGIN
  PERFORM okg.external_action_require_command(p_command,
    ARRAY['deployment','owner','record_id','request_hash','idempotency_key_hash','body','primary_authentication_audit_id','primary_nomos_decision_id','proposal_read_authentication_audit_id','proposal_read_nomos_decision_id'],
    ARRAY['deployment','owner','record_id','request_hash','idempotency_key_hash','body','primary_authentication_audit_id','primary_nomos_decision_id','proposal_read_authentication_audit_id','proposal_read_nomos_decision_id']);
  first_auth := okg.external_automation_authority(p_command, 'external_automation.link', 'primary');
  second_auth := okg.external_automation_authority(p_command, 'external_action.read', 'proposal_read');
  FOREACH field_name IN ARRAY ARRAY['deployment','principal','separation_subject','nomos_provider_id','nomos_subject_binding_id'] LOOP
    IF first_auth->field_name IS DISTINCT FROM second_auth->field_name THEN
      RAISE EXCEPTION 'run_authority_invalid' USING ERRCODE = '42501';
    END IF;
  END LOOP;
  IF p_command->>'primary_authentication_audit_id' = p_command->>'proposal_read_authentication_audit_id'
     OR p_command->>'primary_nomos_decision_id' = p_command->>'proposal_read_nomos_decision_id' THEN
    RAISE EXCEPTION 'run_authority_invalid' USING ERRCODE = '42501';
  END IF;
  PERFORM okg.external_action_require_command(p_command->'body', ARRAY['proposal_id'], ARRAY['proposal_id']);
  IF jsonb_typeof(p_command->'body'->'proposal_id') IS DISTINCT FROM 'string'
     OR p_command->'body'->>'proposal_id' !~ '^[0-9A-HJKMNP-TV-Z]{26}$'
     OR p_command->>'idempotency_key_hash' !~ '^sha256:[0-9a-f]{64}$' THEN
    RAISE EXCEPTION 'invalid_run_record' USING ERRCODE = '22023';
  END IF;
  expected_hash := okg.external_action_hash('okg-external-automation-request-v1', jsonb_build_object('operation','link','deployment',dep,'owner',p_command->>'owner','record_id',p_command->>'record_id','body',p_command->'body'));
  IF expected_hash IS DISTINCT FROM p_command->>'request_hash' THEN
    RAISE EXCEPTION 'run_authority_invalid' USING ERRCODE = '42501';
  END IF;
  -- First row lookup is after BOTH permissions, including on a replay.
  SELECT * INTO r FROM okg.external_automation_runs WHERE deployment_name = dep AND record_id = p_command->>'record_id' AND owner_separation_subject = p_command->>'owner';
  IF NOT FOUND THEN RAISE EXCEPTION 'run_not_found' USING ERRCODE = 'P0002'; END IF;
  SELECT * INTO p FROM okg.external_action_proposals WHERE deployment_name = dep AND proposal_id = p_command->'body'->>'proposal_id' FOR SHARE;
  IF NOT FOUND OR p.proposer_separation_subject <> r.owner_separation_subject
     OR p.automation_id <> r.automation_id OR p.automation_version <> r.automation_version
     OR p.run_id <> r.run_id OR p.correlation_id <> r.correlation_id THEN
    RAISE EXCEPTION 'run_proposal_mismatch' USING ERRCODE = '40001';
  END IF;
  SELECT * INTO prior FROM okg.external_automation_run_proposals WHERE record_id = r.record_id AND owner_separation_subject = r.owner_separation_subject AND link_idempotency_key_hash = p_command->>'idempotency_key_hash';
  IF FOUND THEN
    IF prior.request_hash <> expected_hash THEN RAISE EXCEPTION 'run_identity_conflict' USING ERRCODE = '40001'; END IF;
    RETURN jsonb_build_object('status',200,'body',jsonb_build_object('record_id',r.record_id,'proposal_id',prior.proposal_id,'linked_at',prior.linked_at));
  END IF;
  IF NOT EXISTS (SELECT 1 FROM okg.external_action_restore_fences WHERE deployment_name = dep AND service_mode = 'normal') THEN RAISE EXCEPTION 'run_restore_quarantined' USING ERRCODE = '55000'; END IF;
  IF EXISTS (SELECT 1 FROM okg.external_automation_run_proposals WHERE deployment_name = dep AND proposal_id = p.proposal_id) THEN RAISE EXCEPTION 'run_identity_conflict' USING ERRCODE = '40001'; END IF;
  IF (SELECT count(*) FROM okg.external_automation_run_proposals WHERE record_id = r.record_id) >= 64 THEN RAISE EXCEPTION 'run_link_limit' USING ERRCODE = '54000'; END IF;
  INSERT INTO okg.external_automation_run_proposals(deployment_name,record_id,proposal_id,owner_separation_subject,link_idempotency_key_hash,request_hash,authentication_audit_id,nomos_decision_id,proposal_read_authentication_audit_id,proposal_read_nomos_decision_id)
    VALUES(dep,r.record_id,p.proposal_id,r.owner_separation_subject,p_command->>'idempotency_key_hash',expected_hash,p_command->>'primary_authentication_audit_id',p_command->>'primary_nomos_decision_id',p_command->>'proposal_read_authentication_audit_id',p_command->>'proposal_read_nomos_decision_id') RETURNING * INTO prior;
  RETURN jsonb_build_object('status',201,'body',jsonb_build_object('record_id',r.record_id,'proposal_id',prior.proposal_id,'linked_at',prior.linked_at));
END $run_link$;

-- BEGIN external automation run grants (also converged by views.sql).
REVOKE ALL ON okg.external_automation_runs, okg.external_automation_run_proposals FROM PUBLIC, okg_external_action_api;
GRANT SELECT, INSERT ON okg.external_automation_runs, okg.external_automation_run_proposals TO okg_external_action_function_owner;
DO $run_generation_grant$
BEGIN
  -- Fresh schema precedes views.sql, which unconditionally converges this
  -- grant after creating the authoritative view. Upgrades already have it.
  IF to_regclass('okg.v_generations') IS NOT NULL THEN
    GRANT SELECT (id, deployment_name) ON okg.v_generations TO okg_external_action_function_owner;
  END IF;
END $run_generation_grant$;
REVOKE ALL ON FUNCTION okg.external_automation_transaction_context(), okg.external_automation_register(JSONB), okg.external_automation_read(JSONB), okg.external_automation_link(JSONB), okg.external_automation_authority(JSONB,TEXT,TEXT), okg.external_automation_projection(TEXT) FROM PUBLIC, okg_external_action_api;
DO $run_read_role_revoke$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_ro') THEN
    REVOKE ALL ON FUNCTION okg.external_automation_transaction_context(), okg.external_automation_register(JSONB), okg.external_automation_read(JSONB), okg.external_automation_link(JSONB), okg.external_automation_authority(JSONB,TEXT,TEXT), okg.external_automation_projection(TEXT) FROM app_ro;
  END IF;
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'okg_mcp') THEN
    REVOKE ALL ON FUNCTION okg.external_automation_transaction_context(), okg.external_automation_register(JSONB), okg.external_automation_read(JSONB), okg.external_automation_link(JSONB), okg.external_automation_authority(JSONB,TEXT,TEXT), okg.external_automation_projection(TEXT) FROM okg_mcp;
  END IF;
END $run_read_role_revoke$;
ALTER FUNCTION okg.external_automation_transaction_context() OWNER TO okg_external_action_function_owner;
ALTER FUNCTION okg.external_automation_register(JSONB) OWNER TO okg_external_action_function_owner;
ALTER FUNCTION okg.external_automation_read(JSONB) OWNER TO okg_external_action_function_owner;
ALTER FUNCTION okg.external_automation_link(JSONB) OWNER TO okg_external_action_function_owner;
ALTER FUNCTION okg.external_automation_authority(JSONB,TEXT,TEXT) OWNER TO okg_external_action_function_owner;
ALTER FUNCTION okg.external_automation_projection(TEXT) OWNER TO okg_external_action_function_owner;
GRANT EXECUTE ON FUNCTION okg.external_automation_transaction_context(), okg.external_automation_register(JSONB), okg.external_automation_read(JSONB), okg.external_automation_link(JSONB) TO okg_external_action_api;
GRANT EXECUTE ON FUNCTION okg.external_automation_transaction_context(), okg.external_automation_register(JSONB), okg.external_automation_read(JSONB), okg.external_automation_link(JSONB), okg.external_automation_authority(JSONB,TEXT,TEXT), okg.external_automation_projection(TEXT) TO okg_external_action_function_owner;
-- END external automation run grants.

-- Retain actual event columns and hash when hydrating a command-backed row.
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
    'events',COALESCE((SELECT jsonb_agg(jsonb_build_object('bucket',e.effect_instance_key,'value',okg.external_action_logical_json_record(to_jsonb(e))-'store_record') ORDER BY e.effect_instance_key,e.sequence) FROM okg.external_action_events e WHERE e.deployment_name=bound_deployment),'[]'::jsonb),
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
