-- Build the durable snapshot's proposals, decisions, executions and outcomes
-- from their table columns (issue #2341).
--
-- okg.external_action_store_snapshot returned each row's store_record
-- whenever one existed. For these four tables that record is the original
-- submit, review, claim or outcome command. external_action_review_transition,
-- external_action_claim and external_action_report_outcome update the
-- proposal columns but never its record, so after an approval a freshly
-- hydrated PostgreSQL store saw the proposal as pending at lifecycle version
-- 1. The decision, execution and outcome commands also lack fields their
-- domain records require (a decision's lifecycle_version and payload_hash; an
-- execution's canonical payload, contract digests and deadline; an outcome's
-- recorded_at), so those rows could not be loaded at all.
--
-- The snapshot now overlays every column on the stored record, so the columns
-- win and the record still supplies command-only fields the tables do not
-- carry (proposer_principal, worker_principal, provider_idempotency_key, the
-- claim's idempotency_key_hash). A proposal's created_at, updated_at and
-- expired_at columns stay out of the overlay: the submit response and its
-- idempotent replay are rebuilt from this projection, and adding fields would
-- change the replay bytes of proposals submitted before this migration. A
-- decision is built from its columns alone.
-- An execution also gets its claim lifecycle version (approved + 1, as the
-- claim function commits it), its canonical payload as base64, and its
-- decision's binding hash.
--
-- Why a migration and not only schema.sql: migrate executes schema.sql first
-- and then every pending ordered migration. A database that has not yet
-- applied 0030 or 0033 would get an older snapshot body back from them after
-- schema.sql ran; this file, which runs after both, restores the current
-- body. It is the pattern 0033 and 0034 use. The body below is byte-identical
-- to schema.sql. CREATE OR REPLACE keeps the function's owner and grants. It
-- does not keep SECURITY DEFINER or the search_path setting; they hold
-- because the definition below restates them. Idempotent; no relation or row
-- is touched.
SET lock_timeout = '5s';
SET statement_timeout = '120s';

CREATE OR REPLACE FUNCTION okg.external_action_store_snapshot()
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, okg AS $external_action_store_snapshot_v2$
DECLARE bound_deployment TEXT:=okg.external_action_session_deployment();
BEGIN
  PERFORM okg.external_action_verifier_capability_status();
  PERFORM pg_advisory_xact_lock(hashtextextended('okg.external_action.deployment:'||bound_deployment,0));
  RETURN jsonb_build_object(
    'execution_epoch',COALESCE((SELECT max(epoch) FROM okg.external_action_execution_epoch_leases WHERE deployment_name=bound_deployment),0),
    'execution_epoch_lease_digest',(SELECT envelope_digest FROM okg.external_action_execution_epoch_leases WHERE deployment_name=bound_deployment ORDER BY epoch DESC,lease_sequence DESC LIMIT 1),
    'proposals',COALESCE((SELECT jsonb_agg(jsonb_build_object('key',p.proposal_id,'value',COALESCE(p.store_record,'{}'::jsonb)||(okg.external_action_logical_json_record(to_jsonb(p))-ARRAY['store_record','action_contract_record','submission_idempotency_record','created_at','updated_at','expired_at']::text[])) ORDER BY p.proposal_id) FROM okg.external_action_proposals p WHERE p.deployment_name=bound_deployment),'[]'::jsonb),
    'payload_versions',COALESCE((SELECT jsonb_agg(jsonb_build_object('bucket',v.proposal_id,'value',COALESCE(v.store_record,okg.external_action_logical_json_record(to_jsonb(v))-'store_record')||jsonb_build_object('version',v.payload_version,'difference',v.difference_field_codes,'canonical_payload',jsonb_build_object('__okg_bytes_b64',replace(encode(v.canonical_payload,'base64'),E'\n','')))) ORDER BY v.proposal_id,v.payload_version) FROM okg.external_action_payload_versions v WHERE v.deployment_name=bound_deployment),'[]'::jsonb),
    'auth_audits',COALESCE((SELECT jsonb_agg(jsonb_build_object('key',a.authentication_audit_id,'value',COALESCE(a.store_record,okg.external_action_logical_json_record(to_jsonb(a))-'store_record')) ORDER BY a.authentication_audit_id) FROM okg.external_action_auth_audits a WHERE a.deployment_name=bound_deployment),'[]'::jsonb),
    'decisions',COALESCE((SELECT jsonb_agg(jsonb_build_object('bucket',d.proposal_id,'value',jsonb_build_object('decision_id',d.decision_id,'proposal_id',d.proposal_id,'verdict',CASE WHEN d.verdict='approved' THEN 'approve' ELSE 'reject' END,'lifecycle_version',d.approved_lifecycle_version,'payload_version',d.approved_payload_version,'payload_hash',d.approved_payload_hash,'approver_principal',d.approver_principal,'approver_separation_subject',d.approver_separation_subject,'policy_digest',d.policy_digest,'execute_before',d.execute_before,'reason_code',d.reason_code,'decision_binding_hash',d.decision_binding_hash,'created_at',d.created_at,'authentication_audit_id',d.authentication_audit_id,'nomos_decision_id',d.nomos_decision_id)) ORDER BY d.proposal_id,d.decision_row_id) FROM okg.external_action_decisions d WHERE d.deployment_name=bound_deployment),'[]'::jsonb),
    'transition_idempotency',COALESCE((SELECT jsonb_agg(jsonb_build_object('key',jsonb_build_array(t.deployment_name,t.proposal_id,t.actor_separation_subject,t.idempotency_key_hash),'value',COALESCE(t.store_record,okg.external_action_logical_json_record(to_jsonb(t))-'store_record')) ORDER BY t.transition_idempotency_id) FROM okg.external_action_transition_idempotency t WHERE t.deployment_name=bound_deployment),'[]'::jsonb),
    'executions',COALESCE((SELECT jsonb_agg(jsonb_build_object('key',e.proposal_id,'value',COALESCE(e.store_record,'{}'::jsonb)||(okg.external_action_logical_json_record(to_jsonb(e))-ARRAY['store_record','canonical_payload']::text[])||jsonb_build_object('lifecycle_version',e.approved_lifecycle_version+1,'canonical_payload',jsonb_build_object('__okg_bytes_b64',replace(encode(e.canonical_payload,'base64'),E'\n','')),'decision_binding_hash',(SELECT d.decision_binding_hash FROM okg.external_action_decisions d WHERE d.deployment_name=e.deployment_name AND d.decision_id=e.decision_id),'claim_idempotency_key_hash',e.store_record->>'idempotency_key_hash','handoff_receipt_digest',e.okg_claim_handoff_receipt_digest)) ORDER BY e.execution_id) FROM okg.external_action_executions e WHERE e.deployment_name=bound_deployment),'[]'::jsonb),
    'outcomes',COALESCE((SELECT jsonb_agg(jsonb_build_object('key',o.execution_id,'value',COALESCE(o.store_record,'{}'::jsonb)||(okg.external_action_logical_json_record(to_jsonb(o))-'store_record')) ORDER BY o.outcome_id) FROM okg.external_action_outcomes o WHERE o.deployment_name=bound_deployment),'[]'::jsonb),
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
