"""Audited production-snapshot rebinding for isolated staging environments."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import stat
import subprocess
import tempfile
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping

import psycopg
from okg.substrate.db.connection import connect as db_connect
from psycopg import sql
from psycopg.rows import dict_row

from okg.substrate.db import dsn as dsn_mod
from okg.substrate.deployments.environment import DeploymentEnvironment
from okg.substrate.errors import DeploymentEnvironmentInvalid
from okg.substrate.postgres_restore import timescaledb_restore_context


CLONE_BACKUP_SCHEMA = "okg.environment_clone_backup/v1"
CLONE_REBIND_SCHEMA = "okg.environment_clone_rebind/v1"

# Every application-owned OKG table must be dispositioned. A new table blocks
# cloning until its owner decides whether its rows are portable source truth or
# environment-local authority.
_KNOWN_OKG_TABLES = frozenset(
    {
        "admission_envelopes",
        "admission_events",
        "admission_record_refs",
        "aliases",
        # Derived collision records from alias seeding over the preserved
        # aliases; doctor re-surfaces them at the next seeding pass, so a
        # clone purges rather than copies them.
        "alias_collisions",
        "attribute_effective_attrs",
        "attribute_observations",
        # Storage redesign S2: the overlay materialization marker and overlay
        # ranges are derived from okg.attribute_effective_attrs and this
        # database's generation ordinals; purged with them, rebuilt by the
        # clone's own publishes.
        "attribute_overlay_generations",
        "attribute_overlay_ranges",
        # PACT lane-s-storage-drop-copies: the three attribute copies and the
        # integrity receipts that guarded them. Migration 0046 drops them; the
        # names stay here as purge-if-present so this code still clones a
        # source database without 0046 that has them (the allowlist fails
        # closed on an unlisted table; same ruling as the attr_index names
        # below).
        "attribute_effective_view",
        "attribute_effective_view_intervals",
        "attribute_versions",
        "attribute_effective_integrity_receipts",
        "attribute_effective_integrity_state",
        "attribute_effective_integrity_transitions",
        # schema-attr-filter-index (#3029 for the three that follow it):
        # okg.attr_index and its coverage markers, rebuild queue and derive
        # front. okg#3112 step 4b removed them and migration 0043 drops them;
        # the names stay here as purge-if-present so step-4b code still
        # clones a source database at 0036-0042 that has them (the allowlist
        # fails closed on an unlisted table, as 0025's dropped names showed).
        "attr_index",
        "attr_index_coverage",
        "attr_index_derive_front",
        "attr_index_rebuild_requests",
        # okg#3112 step 2: the declared-attribute expression indexes live on
        # this database's physical `_chronos_*` graph tables, which a clone
        # does not carry. Their manifest, the reconciler state (including
        # whether an operator enabled it) purge; the clone builds its own
        # after an operator's `okg db attr-index reconcile --apply`. Migration
        # 0046 drops the drift-check records; the name stays as
        # purge-if-present for a source database without 0046.
        "declared_attr_index_drift_checks",
        "declared_attr_index_state",
        "declared_attr_indexes",
        # General blob store (issue #3046). Image values in the preserved
        # node_facts name these rows by id, and re-rendering needs the source
        # files, which may not reach the clone -- preserved like
        # visual_crop_blobs.
        "blobs",
        "branch_merge_requests",
        "branches",
        "catalog_versions",
        "chronos_native_graph_storage_marker",
        # Manifest of hot indexes on this database's physical chronos
        # segments; meaningless without the purged `_chronos_*` history.
        "chronos_hot_index_manifest",
        "chronos_graph_edges_current",
        "chronos_graph_nodes_current",
        # Per-checkpoint receipts of destructive history compaction on this
        # database's `_chronos_*` history, which a clone does not carry --
        # purged together with history_compaction_plans below.
        "compacted_generations",
        "consistency_candidate_coverage",
        "consistency_candidates",
        "consistency_check_definitions",
        "consistency_gate_results",
        # Per-host, per-pid connection-pool demand claims (unify-connection-
        # budget); meaningless off the source host.
        "connection_reservations",
        "console_count_rollup",
        # Assignment control, routes, deliveries, and leases are live
        # authority for one coordination environment.  A database clone must
        # start without that authority rather than copying active owners or
        # pending acknowledgements to another machine.
        "coordination_native_control",
        "coordination_native_events",
        # Current approval state is environment-local authority. A restored
        # clone must not carry grants, pending routes, or operator decisions
        # into a database with different identities and policy digests.
        "coordination_native_approvals",
        # Concrete target enrollments are server-owned bindings to a real
        # credential, peer, workspace, and start plan. They are executable
        # authority for this environment, not portable coordination facts;
        # purge them so a clone cannot inherit the source target's identity
        # or route and must be enrolled explicitly after rebinding.
        "coordination_native_target_enrollments",
        "coordination_native_messages",
        "coordination_native_state",
        "database_owner",
        "dbos_bootstrap_marker",
        # Desired-state apply receipts and the worker's convergence
        # acknowledgement (deployment_apply.ensure_deployment_apply_schema,
        # installed at worker boot): runtime authority for THIS environment's
        # supervisor; a clone re-converges from its own desired state.
        "deployment_apply_runs",
        "deployment_worker_ack",
        "deployment_correlations",
        "deployment_cost_caps",
        "deployment_cost_ledger",
        "deployment_write_ownership",
        "deployment_promotion_fences",
        "derived_artifact_definitions",
        "derived_artifact_dependencies",
        "derived_artifact_runs",
        "derived_dirty_rule_audits",
        "derived_dirty_rule_runs",
        "derived_dirty_scopes",
        "derived_output_dependency_index",
        "derived_output_lineage_envelopes",
        "derived_output_manifests",
        "derived_output_slots",
        "derived_relation_registry",
        "derived_run_dependency_bindings",
        "derived_schedule_nodes",
        "derived_schedule_plans",
        "disk_usage_samples",
        "edge_fact_strandings",
        "edge_facts",
        # The deployment's edge ranking (item 92a): configuration that `okg
        # migrate --deployment` and the worker copy from deployment.yaml, so a
        # clone purges it and gets the target deployment's ranking.
        "edge_precedence",
        # Projected live graph read surface, regenerated by publish from the
        # preserved fact log -- same disposition as graph_nodes/graph_edges.
        "edges_live",
        "nodes_live",
        # Crash-recovery journal of the retired attribute-copy recovery.
        # Migration 0046 drops it; the name stays as purge-if-present for a
        # source database without 0046.
        "effective_attribute_recovery_attempts",
        # Vector sets (PACT openrouter-embeddings-hnsw). The registered sets
        # and their content-addressed vectors are paid to rebuild, so a clone
        # preserves them, with the vector counter (preserved vectors keep a
        # seq at or below it).
        "embedding_sets",
        "text_embeddings",
        "embedding_vector_counters",
        # Generation- and row-bound vector-set data: watermarks name source
        # generations, links name node row content, template records are
        # re-written with the links. A backfill rebuilds all three for free
        # (only a document with no vector costs a call), so a clone purges them.
        "embedding_watermarks",
        "node_documents",
        "embedding_templates",
        # Fill completions name source generations too; the clone's first
        # backfill (no paid call when the vectors are preserved) records
        # its own, and search routes to the set from the next publish.
        "embedding_fill_completions",
        "enricher_backfill_progress",
        # Item/signature retry and terminal quarantine are progress authority.
        # Clone purges them with enricher_progress/generation history so the
        # target begins with a full rescan and fresh budgets; rows are never
        # copied or deployment/branch-remapped.
        "enricher_input_quarantines",
        "enricher_progress",
        # Durable compare-and-mint identity authority (#796), preserved so
        # the canonical node ids embedded in the preserved fact log stay
        # pinned across policy changes. Caveat: rows are keyed by the SOURCE
        # deployment name and rebind rewrites only okg.database_owner, so a
        # name-changing rebind leaves them dormant until re-ingest re-claims;
        # preserving is still strictly safer than dropping the ledger.
        "entity_identity_claims",
        # Durable retired-id ledger for bounded entity merges; the retired
        # snapshot is not re-derivable once the live node is retracted, so a
        # clone preserves it.
        "entity_merge_receipts",
        # Source-retirement intent is similarly bound to preserved fact history
        # and its publication generation, so it must travel with that history.
        "source_retirement_receipts",
        # Diagnostic rejection log with occurrence counters; rejections recur
        # when sources re-ingest in the clone, so it purges.
        "entity_resolution_rejections",
        # Generation-scoped reuse-poisoning records; purged together with
        # reuse_certificates below so no stale skip is licensable in a clone.
        "envelope_violations",
        # Per-generation equivalence-harness verdicts over generation history
        # the clone does not carry.
        "equivalence_verdicts",
        "escalation_facts",
        "escalation_live",
        "escalation_schema_marker",
        "external_probe_cache",
        # External-action review, execution, and restore rows are bound to
        # the source deployment's epoch, identities, and operator authorities.
        # A clone must start with no executable review state; the operator
        # creates a fresh login-to-deployment binding after the clone is
        # rebound.  Keeping these names in the closed allow-list also makes a
        # newly installed 0030 table fail closed if this list is not updated.
        "external_automation_runs",
        "external_automation_run_proposals",
        "external_action_proposals",
        "external_action_payload_versions",
        "external_action_decisions",
        "external_action_transition_idempotency",
        "external_action_executions",
        "external_action_outcomes",
        "external_action_events",
        "external_action_auth_audits",
        "external_action_dpop_replay_nonces",
        "external_action_owner_unavailability_attestations",
        "external_action_restore_fences",
        "external_action_restore_recoveries",
        "external_action_execution_epoch_leases",
        "external_action_restore_attestations",
        "external_action_restore_sessions",
        "external_action_restore_checkpoint_staging",
        "external_action_restore_checkpoint_entries",
        "external_action_restore_finalize_idempotency",
        "external_action_restore_completion_receipts",
        "external_action_api_role_bindings",
        # Deployment/branch-scoped historical-backfill work queue; entries are
        # acknowledged only by published checkpoints, which a clone purges.
        "extractor_retry_queue",
        # Receipts recording which admission chunks were applied to THIS
        # database's graph. The clone's graph is purged, so a preserved
        # "applied" receipt would prevent re-application and silently drop the
        # chunk's facts from the rebuilt graph -- purge.
        "fact_batch_receipts",
        "federation_connection_requests",
        "federation_discovered_candidates",
        "federation_mailbox_events",
        "federation_mailbox_messages",
        "federation_peer_trust_pins",
        "federation_route_events",
        "generation_ordinals",
        # Shared-edge declarers (item 92a): Chronos-registered like
        # graph_node_declarers below, purged with the history they describe;
        # the rebuilt graph's writers record them again.
        "graph_edge_declarers",
        # Waiting cross-source edges (cross-source-endpoint-wedge): a
        # Chronos-registered table purged with the history it describes.
        "graph_edge_deferrals",
        "graph_edges",
        "graph_generations",
        # Shared-node declarers (issue #1901): a Chronos-registered table whose
        # rows live in the purged `_chronos_*` history with the graph rows they
        # describe; the rebuilt graph's writers record them again.
        "graph_node_declarers",
        "graph_nodes",
        # schema-grant-coherence. PURGE, both of them, and the reason is the
        # same reason the registry exists: a grant is a decision about THIS
        # database's roles, not portable source truth.
        #
        # `grant_registry`: the substrate rows are re-seeded from views.sql on
        # the clone's first migrate, and the non-substrate rows name relations
        # (runtime-registered `okg.v_current_*` honest-read views) that belong
        # to the purged Chronos history. Copying them would carry a grant
        # decision into an environment whose roles and relations differ.
        # `okg.grant_registry_drift()` reports anything the re-seed misses, so
        # a wrong purge is loud rather than silent.
        "grant_registry",
        # `pending_grants`: a retry queue for grants THIS database could not
        # apply. It is per-database operational state by construction --
        # replaying another environment's failures would queue statements
        # against relations the clone may not have.
        "pending_grants",
        # Dry-run/apply authority and operator pins for policy-driven Chronos
        # history compaction; both name checkpoints in the purged `_chronos_*`
        # history, so they purge with it (see compacted_generations above).
        "history_compaction_plans",
        "history_generation_pins",
        "identifier_patterns",
        "invariants",
        "ir_invocations",
        "ir_program_bindings",
        "ir_programs",
        "job_chunks",
        "job_definitions",
        "job_dependencies",
        "job_external_calls",
        "job_item_frontiers",
        "job_items",
        "job_runs",
        "job_runtime_ledger_retirement_marker",
        "lease_audit",
        # One-row marker that the legacy runtime was retired in this
        # database; purged like the other runtime markers (role_split_marker,
        # dbos_bootstrap_marker), the retirement migration re-stamps it.
        "legacy_runtime_retirement_marker",
        "lens_definitions",
        "lens_overlay_log",
        "lens_putback_deltas",
        "lens_putback_requests",
        "link_cache",
        # Deployment/branch-scoped alias-miss strand records (#685); rebuilt
        # by re-extraction and acknowledged only by published checkpoints, so
        # a clone purges them like link_cache.
        "link_retry",
        "llm_calls",
        "mcp_call_log",
        "mcp_trace_events",
        # Bounded exact-digest replay cache for model-backed invocations
        # (consistency-model-outcome-replay). Rows are keyed by the source
        # deployment name, so they are dead after a name-changing rebind;
        # purging costs re-billed calls on replay, never correctness.
        # Operator call samples (issue #2011): three retention classes plus
        # their watermark. Samples belong to the environment that made the
        # calls, never to a clone of it.
        "call_sample_cold",
        "call_sample_hot",
        "call_sample_warm",
        "call_sample_watermark",
        "model_outcomes",
        "narrowings",
        "node_facts",
        "node_series",
        "node_series_watermark",
        "nomos_config_activations",
        "nomos_config_sets",
        "nomos_decisions",
        "nomos_external_claims",
        "nomos_identity_providers",
        "nomos_permission_grants",
        "nomos_policy_activations",
        "nomos_policy_bundles",
        # PACT lane-h-nomos-read-class-sql: the content-addressed catalog of
        # the read classes stamped on graph row versions. It describes the
        # `_chronos_*` graph rows, which a clone purges, so it purges with
        # them; the clone's writers re-insert each class as they stamp it.
        "nomos_read_classes",
        "nomos_subject_bindings",
        "observation_current",
        "observation_events",
        "ontology_rollup",
        # schema-dead-weight-retirement DROPPED okg.pagerank_scores and
        # okg.verifier_runs (migration 0025), but these two entries STAY. This
        # set is a fail-closed allowlist: an okg table that is not listed makes
        # clone_table_disposition return "unknown", which refuses the whole
        # clone. A deployment that has not yet taken 0025 still has both
        # tables, so removing the names would break cloning for exactly the
        # databases that are behind on migrations. Keeping them costs nothing
        # once the tables are gone -- a name that matches no relation is never
        # consulted -- and they are purge-disposition either way.
        "pagerank_scores",
        # PACT ledger. It moved out of `public` into `okg` under
        # schema-namespace-qualification; the disposition does not change with
        # the namespace -- these rows are environment-local authority (which
        # worktree claimed what, in which checkout), so a clone purges them,
        # exactly as the `public`-schema clause below did before the move. The
        # rebuild-loss policy's export/re-admit path is a separate mechanism.
        "pact_coordination_events",
        "pact_decisions",
        "pact_intents",
        "poros_delivery_attempts",
        "poros_detached_object_manifests",
        "poros_inbox_messages",
        "poros_incorporation_events",
        "poros_outbox_messages",
        "poros_replay_cache",
        "post_publish_projection_status",
        "producer_profiles",
        "projection_pending",
        # Deployment/checkpoint-scoped remediation state and its idempotency
        # receipts; both describe repair work against the purged graph.
        "projection_remediation_batches",
        "projection_remediation_runs",
        # Per-(deployment, generation) claim-plane selections (consistency
        # spec 9.8); the generation history they annotate is purged.
        "repair_selections",
        # Per-generation skip justifications (consistency spec 9.5); a
        # preserved certificate could license skipping work the clone's fresh
        # graph has not actually done -- purge.
        "reuse_certificates",
        "role_split_marker",
        "runtime_authority_receipts",
        # Operator dispositions of THIS environment's retained DBOS terminal
        # failures; the dbos rows they acknowledge are purged, and a carried
        # acknowledgement could mark a recurring staging failure as already
        # dispositioned -- purge.
        "runtime_dlq_acknowledgement_events",
        # Typed DBOS workflow evidence is environment-local and must not be
        # inherited by a cloned staging environment.
        "workflow_operational_registrations",
        "workflow_operational_outcomes",
        # Exact watermark of multi-phase data motion for the bytes the dump
        # carries (schema-migration-hardening); the multiphase driver refuses
        # when it is absent -- preserved like schema_migrations.
        "schema_migration_phases",
        "schema_migrations",
        "source_cursor_blobs",
        "source_progress",
        # Indexed record authority v2 (issue #1325), the successor storage for
        # what source_record_set_blobs holds inline. A sealed authority is the
        # per-source record of exactly which records a run was responsible for,
        # and no source can rebuild it without a full re-read, so the whole
        # sealed structure travels with the clone exactly as the legacy blob
        # does: the header, its ordered operations, the live record index, the
        # component slots and their field-to-digest map, and the
        # content-addressed payload rows the fields point at.
        "source_record_authorities",
        "source_record_authority_operations",
        "source_record_authority_records",
        "source_record_authority_components",
        "source_record_authority_component_fields",
        "source_record_authority_payloads",
        "source_record_authority_payload_chunks",
        # The identity index is derived from the component fields, but this
        # change ships no rebuild path and the verified reader refuses an
        # authority whose index does not agree with its components in both
        # directions. Purging it would leave every preserved authority
        # unreadable, so it is preserved with them rather than treated as a
        # rebuildable projection like the interval tables.
        "source_record_authority_identities",
        # The single public sealed reference per source. A preserved authority
        # with no ref row is orphaned: the reader requires the exact current
        # binding and refuses a bare sealed uuid.
        "source_record_authority_refs",
        # The transaction-private review handoff, keyed by admission_id and
        # deleted when the decision accepts or rejects. It is a claim that THIS
        # environment staged an authority and has not yet decided it, so a
        # clone must not inherit it and bind authority against a decision made
        # somewhere else. Consequence, stated rather than buried: a review-mode
        # admission still pending at clone time will not seal its authority in
        # the clone -- the authority stays in `staging` and the source must be
        # re-run there. That is the fail-closed direction, and staged rows are
        # over-kept by design (retention is issues #1308 and #1309).
        "source_record_authority_pending_admissions",
        # Ordered pieces of a large record set (issue #3008); unreadable
        # without their header row above, so they travel with it.
        "source_record_set_blob_pieces",
        "source_record_set_blobs",
        "source_record_sets",
        "source_watermarks",
        "subtype_ancestors",
        "subtypes",
        "telemetry_artifacts",
        "telemetry_events",
        # Bounded graph-effect telemetry keyed by preserved trace/span ids --
        # preserved with the rest of the telemetry family.
        "telemetry_graph_effect_manifests",
        "telemetry_graph_effects",
        "telemetry_metrics",
        "telemetry_spans",
        "telemetry_traces",
        # Retired by migration 0025; retained here for un-migrated
        # deployments (see the pagerank_scores note above).
        "verifier_runs",
        # Content-keyed VLM retry-budget ledger; purging it would re-arm the
        # retry cost it exists to bound, so it travels with the clone.
        "visual_caption_failures",
        # Content-addressed figure bytes staged between discovery and
        # captioning so the enricher never re-opens the PDF; staging may not
        # reach the source corpus -- preserved like source_record_set_blobs.
        "visual_crop_blobs",
        # Per-(deployment, generation) work/cost accounting (consistency spec
        # 9.9); environment-local like deployment_cost_ledger.
        "work_ledger",
    }
)

_PRESERVE_OKG_TABLES = frozenset(
    {
        "admission_envelopes",
        "admission_events",
        "admission_record_refs",
        "aliases",
        "attribute_observations",
        "blobs",
        "catalog_versions",
        "consistency_check_definitions",
        "derived_artifact_definitions",
        "derived_artifact_dependencies",
        "derived_relation_registry",
        "edge_facts",
        # Vector sets: see _KNOWN_OKG_TABLES.
        "embedding_sets",
        "embedding_vector_counters",
        "text_embeddings",
        "entity_identity_claims",
        "entity_merge_receipts",
        "source_retirement_receipts",
        "escalation_facts",
        "identifier_patterns",
        "invariants",
        "ir_program_bindings",
        "ir_programs",
        "job_definitions",
        "job_dependencies",
        "lens_definitions",
        "narrowings",
        "node_facts",
        "node_series",
        "producer_profiles",
        "schema_migration_phases",
        "schema_migrations",
        # Indexed record authority v2 (issue #1325): the sealed authority and
        # everything needed to read it. Same argument as
        # source_record_set_blobs directly above -- see _KNOWN_OKG_TABLES for
        # the per-table reasoning, including why the identity index and the
        # public ref travel with it and why pending_admissions does not.
        "source_record_authorities",
        "source_record_authority_operations",
        "source_record_authority_records",
        "source_record_authority_components",
        "source_record_authority_component_fields",
        "source_record_authority_payloads",
        "source_record_authority_payload_chunks",
        "source_record_authority_identities",
        "source_record_authority_refs",
        # Ordered pieces of a large record set (issue #3008); unreadable
        # without their header row above, so they travel with it.
        "source_record_set_blob_pieces",
        "source_record_set_blobs",
        "subtype_ancestors",
        "subtypes",
        "telemetry_artifacts",
        "telemetry_events",
        "telemetry_graph_effect_manifests",
        "telemetry_graph_effects",
        "telemetry_metrics",
        "telemetry_spans",
        "telemetry_traces",
        "visual_caption_failures",
        "visual_crop_blobs",
    }
)


def claim_environment_database_owner(
    environment: DeploymentEnvironment,
    *,
    apply: bool,
) -> dict[str, Any]:
    """Claim an unstamped database only when all resident generations agree."""
    from okg.substrate.db.deployment_owner import claim_db_owner

    with db_connect(environment.database.dsn) as conn, conn.cursor() as cur:
        if apply:
            claim = claim_db_owner(
                cur,
                environment.graph_deployment_name,
                command="okg environment claim-owner",
            )
            conn.commit()
        else:
            from okg.substrate.db.deployment_owner import (
                foreign_published_deployments,
                read_db_owner,
            )

            owner = read_db_owner(cur)
            foreign = foreign_published_deployments(
                cur,
                environment.graph_deployment_name,
            )
            if owner not in {None, environment.graph_deployment_name}:
                raise _error(
                    "database is already owned by another deployment",
                    expected=environment.graph_deployment_name,
                    observed=owner,
                )
            if owner is None and foreign:
                raise _error(
                    "unstamped database contains foreign published deployments",
                    expected=environment.graph_deployment_name,
                    observed=foreign,
                )
            claim = {
                "owner_before": owner,
                "owner_after": owner,
                "foreign_published_deployments": foreign,
            }
        return {
            "schema": "okg.environment_database_owner_claim/v1",
            "ok": (
                claim["owner_after"] == environment.graph_deployment_name
                if apply
                else True
            ),
            "apply": apply,
            "environment_id": environment.id,
            "database": environment.database.name,
            "expected_owner": environment.graph_deployment_name,
            "owner_before": claim["owner_before"],
            "owner_after": claim["owner_after"],
            "foreign_published_deployments": claim["foreign_published_deployments"],
        }


def clone_table_disposition(schema: str, table: str) -> str:
    """Return preserve/rewrite/purge, or unknown for a fail-closed table."""
    if schema == "okg":
        if table == "database_owner":
            return "rewrite"
        if table.startswith("nomos_decisions_backup_"):
            return "purge"
        if table not in _KNOWN_OKG_TABLES:
            return "unknown"
        return "preserve" if table in _PRESERVE_OKG_TABLES else "purge"
    if schema == "dbos":
        # DBOS owns this schema version ledger. Preserve it so a restored
        # empty runtime recognizes the installed table shape; all workflow,
        # schedule, event, and output rows remain environment-local.
        return "preserve" if table == "dbos_migrations" else "purge"
    if schema == "okg_derived":
        return "purge"
    if schema == "public" and (
        table.startswith("_chronos_") or table.startswith("pact_")
    ):
        return "purge"
    if schema == "public" and table == "okg_ephemeral_database":
        # Janitor droppability registration (EPHEMERAL_MARKER_TABLE in
        # db/management.py). It asserts THIS database may be swept, so it
        # never travels with a clone; losing it only makes the janitor
        # fail safe (the database survives).
        return "purge"
    return "unknown"


def create_environment_clone_backup(
    *,
    source: DeploymentEnvironment,
    target: DeploymentEnvironment,
    dump_path: str | Path,
    report_path: str | Path,
    apply: bool,
) -> dict[str, Any]:
    _validate_pair(source, target)
    dump = Path(dump_path).expanduser().resolve()
    report_target = Path(report_path).expanduser().resolve()
    # `idle_in_transaction_session_timeout=0` disables the bound for THIS
    # session only. `pg_export_snapshot()` is valid only while the exporting
    # transaction is still open, so this REPEATABLE READ transaction has to
    # stay open for the whole `pg_dump --snapshot=` run below. That run is
    # client-side work between statements, which leaves the session idle in
    # transaction for as long as the dump takes -- far past the 300-second
    # default on any real database. Letting the bound fire would terminate the
    # exporting backend with SQLSTATE 25P03, invalidate the snapshot, and fail
    # the dump. The factory's other protections (keepalives, connect_timeout)
    # still apply, and libpq takes the last `-c` for a setting, so this
    # overrides the factory's own value.
    with db_connect(
        source.database.dsn,
        row_factory=dict_row,
        options="-c idle_in_transaction_session_timeout=0",
    ) as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        snapshot = conn.execute("SELECT pg_export_snapshot() AS snapshot").fetchone()[
            "snapshot"
        ]
        inventory = _table_inventory(conn)
        identity = _database_identity(conn)
        physical_purge_relations = _timescale_purge_relations(conn, inventory)
        _require_source_identity(source, identity)
        payload: dict[str, Any] = {
            "schema": CLONE_BACKUP_SCHEMA,
            "ok": True,
            "apply": apply,
            "captured_at": datetime.now(timezone.utc).isoformat(),
            "source": {
                "environment_id": source.id,
                "graph_deployment_name": source.graph_deployment_name,
                **identity,
            },
            "target": {
                "environment_id": target.id,
                "graph_deployment_name": target.graph_deployment_name,
                "database": target.database.name,
            },
            "definition_digest": source.definition_digest,
            "tables": inventory,
            "physical_purge_relations": physical_purge_relations,
            "class_counts": _class_counts(inventory),
            "dump": {"path": str(dump), "digest": None, "format": "custom"},
        }
        if not apply:
            return payload
        pg_dump = _tool("pg_dump")
        dump.parent.mkdir(parents=True, exist_ok=True)
        dump.touch(mode=0o600, exist_ok=True)
        dump.chmod(0o600)
        database, child_env = _libpq_command_target(source.database.dsn)
        argv = [
            pg_dump,
            "--format=custom",
            "--compress=6",
            "--no-owner",
            "--no-privileges",
            f"--snapshot={snapshot}",
            "--file",
            str(dump),
            database,
        ]
        excluded = [
            row["table"] for row in inventory if row["disposition"] == "purge"
        ]
        excluded.extend(physical_purge_relations)
        for table in sorted(set(excluded)):
            argv.insert(-1, f"--exclude-table-data={table}")
        _run(argv, operation="clone backup", env=child_env)
        dump.chmod(0o600)
        payload["dump"]["digest"] = _file_digest(dump)
        _atomic_write_json(report_target, payload)
        return payload


def restore_environment_clone(
    *,
    target: DeploymentEnvironment,
    report_path: str | Path,
    apply: bool,
    isolation_evidence: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    report = _load_backup_report(report_path)
    _validate_target(report, target)
    dump = Path(report["dump"]["path"]).expanduser().resolve()
    expected_digest = str(report["dump"].get("digest") or "")
    if not expected_digest or _file_digest(dump) != expected_digest:
        raise _error("clone backup digest does not match its report")
    payload = {
        "schema": "okg.environment_clone_restore/v1",
        "ok": True,
        "apply": apply,
        "environment_id": target.id,
        "database": target.database.name,
        "dump_digest": expected_digest,
        "timescaledb_version": report["source"]["timescaledb_version"],
    }
    if not apply:
        return payload
    fence = _validate_clone_isolation_evidence(
        target,
        isolation_evidence,
    )
    _recreate_database(target.database.dsn, expected_database=target.database.name)
    database, child_env = _libpq_command_target(target.database.dsn)
    existing_options = child_env.get("PGOPTIONS", "").strip()
    child_env["PGOPTIONS"] = " ".join(
        part
        for part in (
            existing_options,
            "-c default_transaction_read_only=off "
            "-c max_parallel_maintenance_workers=0",
        )
        if part
    )
    with timescaledb_restore_context(
        target.database.dsn,
        version=report["source"]["timescaledb_version"],
    ):
        _run(
            [
                _tool("pg_restore"),
                "--exit-on-error",
                "--no-owner",
                "--no-privileges",
                "--dbname",
                database,
                str(dump),
            ],
            operation="clone restore",
            env=child_env,
        )
    payload["host_service_fence"] = fence
    payload["ordinary_sessions_read_only"] = True
    return payload


def rebind_environment_clone(
    *,
    target: DeploymentEnvironment,
    report_path: str | Path,
    output_path: str | Path,
    apply: bool,
) -> dict[str, Any]:
    report = _load_backup_report(report_path)
    _validate_target(report, target)
    with db_connect(
        _writable_clone_dsn(target.database.dsn),
        row_factory=dict_row,
    ) as conn:
        inventory = _table_inventory(conn)
        _verify_restored_inventory(inventory, report["tables"])
        identity = _database_identity(conn)
        source = report["source"]
        for field in ("owner_deployment_name", "db_owner_uuid", "schema_fingerprint"):
            if identity.get(field) != source.get(field):
                raise _error(
                    "restored clone source identity does not match backup report",
                    field=field,
                    expected=source.get(field),
                    observed=identity.get(field),
                )
        payload: dict[str, Any] = {
            "schema": CLONE_REBIND_SCHEMA,
            "ok": True,
            "apply": apply,
            "captured_at": datetime.now(timezone.utc).isoformat(),
            "source_backup_digest": report["dump"]["digest"],
            "source": dict(source),
            "target": {
                "environment_id": target.id,
                "graph_deployment_name": target.graph_deployment_name,
                "database": target.database.name,
            },
            "preserved": _rows_for(inventory, "preserve"),
            "rewritten": _rows_for(inventory, "rewrite"),
            "purged": _rows_for(report["tables"], "purge"),
            "regenerate": _regeneration_inventory(inventory),
        }
        if not apply:
            return payload
        with conn.transaction():
            conn.execute(
                """
                UPDATE okg.database_owner
                   SET owner_deployment_name = %s,
                       db_owner_uuid = gen_random_uuid(),
                       stamped_at = now(),
                       stamped_by_command = %s
                 WHERE singleton
                """,
                (target.graph_deployment_name, "okg environment clone-rebind"),
            )
        rebound = _database_identity(conn)
        if rebound["owner_deployment_name"] != target.graph_deployment_name:
            raise _error("staging database ownership rewrite did not persist")
        payload["rewritten"] = [
            {
                "table": "okg.database_owner",
                "rows": 1,
                "from": source["owner_deployment_name"],
                "to": target.graph_deployment_name,
                "db_owner_uuid": rebound["db_owner_uuid"],
            }
        ]
    payload["removed_runtime_files"] = _clear_staging_runtime_authority(target)
    _clear_database_read_only_default(
        target.database.dsn,
        expected_database=target.database.name,
    )
    payload["ordinary_sessions_read_only"] = False
    _atomic_write_json(Path(output_path).expanduser().resolve(), payload)
    return payload


def finalize_environment_clone_rebind_report(
    target: DeploymentEnvironment,
) -> dict[str, Any]:
    """Replace pending regeneration expectations with observed live counts."""
    if not target.readiness.requires_clone_rebind:
        return {"ok": True, "status": "not_required"}
    path = target.runtime.state_root / "clone-rebind.json"
    try:
        if path.is_symlink() or not path.is_file():
            raise _error("clone-rebind report is missing for regeneration proof")
        if stat.S_IMODE(path.stat().st_mode) != 0o600:
            raise _error("clone-rebind report mode is not 0600")
        report = json.loads(path.read_text(encoding="utf-8"))
        if (
            not isinstance(report, dict)
            or report.get("schema") != CLONE_REBIND_SCHEMA
            or (report.get("target") or {}).get("environment_id") != target.id
        ):
            raise _error("clone-rebind report is invalid or foreign")
        with db_connect(target.database.dsn, row_factory=dict_row) as conn:
            role_count = int(
                conn.execute(
                    """
                    SELECT count(*) AS count
                      FROM pg_roles
                     WHERE rolname = ANY(%s) AND rolcanlogin
                    """,
                    (["app_ro", "app_rw", "okg_mcp"],),
                ).fetchone()["count"]
            )
            nomos_rows = _relation_row_total(
                conn,
                (
                    "okg.nomos_config_activations",
                    "okg.nomos_config_sets",
                    "okg.nomos_policy_activations",
                    "okg.nomos_policy_bundles",
                ),
            )
            source_rows = _relation_row_total(
                conn,
                ("okg.source_progress", "okg.source_watermarks"),
            )
            schedule_rows = _relation_row_total(
                conn,
                ("dbos.workflow_schedules",),
            )
        credentials_path = (
            target.runtime.state_root
            / "supervisor/database-role-credentials.json"
        )
        verified_role_count = _verified_database_role_count(
            target,
            credentials_path,
        )
        mcp_path = target.runtime.state_root / "supervisor/mcp-registration.json"
        readiness_path = target.readiness.evidence_path
        readiness = _load_readiness_evidence(readiness_path)
        observed_at = datetime.now(timezone.utc).isoformat()
        regeneration = [
            {
                "class": "database_credentials",
                "status": "completed",
                "rows": verified_role_count,
                "declared_roles": role_count,
                "resource_count": int(credentials_path.is_file()),
            },
            {
                "class": "nomos_baseline",
                "status": "completed",
                "rows": nomos_rows,
            },
            {
                "class": "source_progress",
                "status": "completed",
                "rows": source_rows,
            },
            {
                "class": "dbos_schedules",
                "status": "completed",
                "rows": schedule_rows,
            },
            {
                "class": "mcp_registration",
                "status": "completed",
                "rows": int(mcp_path.is_file()),
            },
            {
                "class": "readiness_evidence",
                "status": "completed",
                "rows": int(readiness_path.is_file()),
            },
        ]
        ok = bool(
            role_count == 3
            and verified_role_count == 3
            and credentials_path.is_file()
            and nomos_rows > 0
            and source_rows > 0
            and schedule_rows > 0
            and mcp_path.is_file()
            and readiness.get("ok") is True
            and readiness.get("status") == "ready"
        )
        report["regenerate"] = regeneration
        report["regeneration_observed_at"] = observed_at
        report["regeneration_status"] = "completed" if ok else "blocked"
        _atomic_write_json(path, report)
        return {
            "ok": ok,
            "status": report["regeneration_status"],
            "path": str(path),
            "observed_at": observed_at,
            "regenerate": regeneration,
        }
    except Exception as exc:  # noqa: BLE001 - readiness needs a blocker payload.
        return {
            "ok": False,
            "status": "unavailable",
            "path": str(path),
            "error": type(exc).__name__,
            "message": str(exc),
        }


def _relation_row_total(
    conn: psycopg.Connection[Any],
    relations: tuple[str, ...],
) -> int:
    total = 0
    for relation in relations:
        schema, table = relation.split(".", 1)
        present = conn.execute("SELECT to_regclass(%s) AS relation", (relation,)).fetchone()
        if present is None or present["relation"] is None:
            continue
        total += int(
            conn.execute(
                sql.SQL("SELECT count(*) AS count FROM {}.{}").format(
                    sql.Identifier(schema),
                    sql.Identifier(table),
                )
            ).fetchone()["count"]
        )
    return total


def _load_readiness_evidence(path: Path) -> dict[str, Any]:
    if path.is_symlink() or not path.is_file():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    if not isinstance(raw, dict):
        return {}
    return raw


def _verified_database_role_count(
    target: DeploymentEnvironment,
    path: Path,
) -> int:
    if path.is_symlink() or not path.is_file():
        return 0
    if stat.S_IMODE(path.stat().st_mode) != 0o600:
        return 0
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return 0
    passwords = raw.get("passwords") if isinstance(raw, dict) else None
    roles = ("app_ro", "app_rw", "okg_mcp")
    if (
        not isinstance(raw, dict)
        or raw.get("schema")
        != "okg.environment_database_role_credentials/v1"
        or raw.get("environment_id") != target.id
        or not isinstance(passwords, dict)
        or set(passwords) != set(roles)
        or any(not isinstance(passwords[role], str) for role in roles)
    ):
        return 0
    verified = 0
    for role in roles:
        dsn = (
            dsn_mod.parse(target.database.dsn)
            .with_params(user=role, password=passwords[role])
            .render()
        )
        with db_connect(dsn, autocommit=True) as conn:
            current = conn.execute("SELECT current_user").fetchone()
        observed = (
            current[0]
            if not isinstance(current, Mapping)
            else current.get("current_user")
        )
        if observed != role:
            return verified
        verified += 1
    return verified


def _table_inventory(conn: psycopg.Connection[Any]) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT schemaname, tablename
          FROM pg_tables
         WHERE schemaname IN ('okg', 'dbos', 'okg_derived', 'public')
         ORDER BY schemaname, tablename
        """
    ).fetchall()
    inventory: list[dict[str, Any]] = []
    unknown: list[str] = []
    for row in rows:
        schema = str(row["schemaname"])
        table = str(row["tablename"])
        disposition = clone_table_disposition(schema, table)
        qualified = f"{schema}.{table}"
        if disposition == "unknown":
            unknown.append(qualified)
            continue
        count = conn.execute(
            sql.SQL("SELECT count(*) AS count FROM {}.{}").format(
                sql.Identifier(schema), sql.Identifier(table)
            )
        ).fetchone()["count"]
        inventory.append(
            {"table": qualified, "disposition": disposition, "rows": int(count)}
        )
    if unknown:
        raise _error(
            "clone inventory contains unclassified application tables",
            unknown_tables=unknown,
        )
    return inventory


def _verify_restored_inventory(
    observed: list[dict[str, Any]],
    source: list[dict[str, Any]],
) -> None:
    source_by_table = {row["table"]: row for row in source}
    observed_by_table = {row["table"]: row for row in observed}
    if set(source_by_table) != set(observed_by_table):
        raise _error(
            "restored clone table inventory differs from backup",
            missing=sorted(set(source_by_table) - set(observed_by_table)),
            extra=sorted(set(observed_by_table) - set(source_by_table)),
        )
    mismatches: list[dict[str, Any]] = []
    for table, expected in source_by_table.items():
        row = observed_by_table[table]
        wanted = expected["rows"] if expected["disposition"] != "purge" else 0
        if row["disposition"] != expected["disposition"] or row["rows"] != wanted:
            mismatches.append(
                {
                    "table": table,
                    "expected_disposition": expected["disposition"],
                    "observed_disposition": row["disposition"],
                    "expected_rows": wanted,
                    "observed_rows": row["rows"],
                }
            )
    if mismatches:
        raise _error("restored clone does not match selective backup", mismatches=mismatches)


def _timescale_purge_relations(
    conn: psycopg.Connection[Any],
    inventory: list[dict[str, Any]],
) -> list[str]:
    dispositions = {row["table"]: row["disposition"] for row in inventory}
    extension = conn.execute(
        "SELECT to_regclass('_timescaledb_catalog.hypertable') AS relation"
    ).fetchone()
    if extension is None or extension["relation"] is None:
        return []
    columns: dict[str, set[str]] = {}
    for row in conn.execute(
        """
        SELECT relation.relname AS relation, attribute.attname AS column
          FROM pg_catalog.pg_attribute attribute
          JOIN pg_catalog.pg_class relation ON relation.oid = attribute.attrelid
          JOIN pg_catalog.pg_namespace namespace ON namespace.oid = relation.relnamespace
         WHERE namespace.nspname = '_timescaledb_catalog'
           AND relation.relname IN ('hypertable', 'chunk', 'compression_settings')
           AND attribute.attnum > 0 AND NOT attribute.attisdropped
        """
    ).fetchall():
        columns.setdefault(row["relation"], set()).add(row["column"])
    parent_columns = columns.get("hypertable", set())
    chunk_columns = columns.get("chunk", set())
    if (
        not {"id", "schema_name", "table_name"} <= parent_columns
        or not {"hypertable_id"} <= chunk_columns
    ):
        raise _error("unsupported Timescale catalog for clone exclusions")
    if "relid" in chunk_columns:
        if not {"relid", "compress_relid"} <= columns.get(
            "compression_settings", set()
        ):
            raise _error(
                "unsupported Timescale compression catalog for clone exclusions"
            )
        query = """
        SELECT parent.schema_name AS parent_schema, parent.table_name AS parent_table,
               chunk.relid AS chunk_reference,
               chunk_ns.nspname AS chunk_schema, chunk_rel.relname AS chunk_table,
               settings.compress_relid AS compressed_reference,
               compressed_ns.nspname AS compressed_schema,
               compressed_rel.relname AS compressed_table,
               NULL AS compressed_parent_reference,
               NULL AS compressed_parent_schema, NULL AS compressed_parent_table
          FROM _timescaledb_catalog.hypertable parent
          LEFT JOIN _timescaledb_catalog.chunk chunk ON chunk.hypertable_id = parent.id
          LEFT JOIN pg_catalog.pg_class chunk_rel ON chunk_rel.oid = chunk.relid
          LEFT JOIN pg_catalog.pg_namespace chunk_ns ON chunk_ns.oid = chunk_rel.relnamespace
          LEFT JOIN _timescaledb_catalog.compression_settings settings ON settings.relid = chunk.relid
          LEFT JOIN pg_catalog.pg_class compressed_rel ON compressed_rel.oid = settings.compress_relid
          LEFT JOIN pg_catalog.pg_namespace compressed_ns ON compressed_ns.oid = compressed_rel.relnamespace
         ORDER BY 1, 2, 4, 5
        """
    elif {
        "id",
        "schema_name",
        "table_name",
        "compressed_chunk_id",
    } <= chunk_columns and ("compressed_hypertable_id" in parent_columns):
        query = """
        SELECT parent.schema_name AS parent_schema, parent.table_name AS parent_table,
               chunk.id AS chunk_reference,
               chunk.schema_name AS chunk_schema, chunk.table_name AS chunk_table,
               chunk.compressed_chunk_id AS compressed_reference,
               compressed.schema_name AS compressed_schema, compressed.table_name AS compressed_table,
               parent.compressed_hypertable_id AS compressed_parent_reference,
               compressed_parent.schema_name AS compressed_parent_schema,
               compressed_parent.table_name AS compressed_parent_table
          FROM _timescaledb_catalog.hypertable parent
          LEFT JOIN _timescaledb_catalog.chunk chunk ON chunk.hypertable_id = parent.id
          LEFT JOIN _timescaledb_catalog.chunk compressed ON compressed.id = chunk.compressed_chunk_id
          LEFT JOIN _timescaledb_catalog.hypertable compressed_parent
            ON compressed_parent.id = parent.compressed_hypertable_id
         ORDER BY 1, 2, 4, 5
        """
    else:
        raise _error("unsupported Timescale catalog for clone exclusions")
    physical: set[str] = set()
    for row in conn.execute(query).fetchall():
        parent = f"{row['parent_schema']}.{row['parent_table']}"
        if dispositions.get(parent) != "purge":
            continue
        for prefix in ("chunk", "compressed", "compressed_parent"):
            reference, schema, table = (
                row[f"{prefix}_reference"],
                row[f"{prefix}_schema"],
                row[f"{prefix}_table"],
            )
            if reference is not None and (not schema or not table):
                raise _error(
                    "unresolved Timescale relation for clone exclusions", table=parent
                )
            if schema and table:
                physical.add(f"{schema}.{table}")
    return sorted(physical)


def _database_identity(conn: psycopg.Connection[Any]) -> dict[str, Any]:
    database = conn.execute("SELECT current_database() AS database").fetchone()
    owner = conn.execute(
        """
        SELECT owner_deployment_name, db_owner_uuid::text, schema_fingerprint
          FROM okg.database_owner
         WHERE singleton
        """
    ).fetchone()
    if owner is None:
        raise _error("database owner stamp is missing")
    migrations = conn.execute(
        """
        SELECT migration_id, checksum, status
          FROM okg.schema_migrations
         ORDER BY migration_id
        """
    ).fetchall()
    extension = conn.execute(
        "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'"
    ).fetchone()
    if extension is None or not extension["extversion"]:
        raise _error("database does not have a TimescaleDB version")
    fingerprint = hashlib.sha256(
        json.dumps(migrations, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()
    return {
        "database": database["database"],
        "owner_deployment_name": owner["owner_deployment_name"],
        "db_owner_uuid": owner["db_owner_uuid"],
        "schema_fingerprint": owner["schema_fingerprint"],
        "migration_inventory_digest": f"sha256:{fingerprint}",
        "timescaledb_version": extension["extversion"],
    }


def _validate_pair(
    source: DeploymentEnvironment,
    target: DeploymentEnvironment,
) -> None:
    if source.tier != "production" or target.tier != "staging":
        raise _error("clone backup requires production source and staging target")
    if source.definition_digest != target.definition_digest:
        raise _error("clone environments do not share one definition digest")
    if source.database.name == target.database.name or source.database.dsn == target.database.dsn:
        raise _error("clone source and target database identities overlap")


def _require_source_identity(
    source: DeploymentEnvironment,
    identity: Mapping[str, Any],
) -> None:
    if identity.get("database") != source.database.name:
        raise _error("source DSN resolved an unexpected database")
    if identity.get("owner_deployment_name") != source.graph_deployment_name:
        raise _error("source database ownership does not match production")


def _validate_target(report: Mapping[str, Any], target: DeploymentEnvironment) -> None:
    if target.tier != "staging":
        raise _error("clone restore and rebind require a staging target")
    expected = report.get("target") or {}
    mismatches = {
        key: {"expected": wanted, "observed": observed}
        for key, wanted, observed in (
            ("environment_id", expected.get("environment_id"), target.id),
            ("graph_deployment_name", expected.get("graph_deployment_name"), target.graph_deployment_name),
            ("database", expected.get("database"), target.database.name),
            ("definition_digest", report.get("definition_digest"), target.definition_digest),
        )
        if wanted != observed
    }
    if mismatches:
        raise _error("clone report belongs to another target", mismatches=mismatches)


def _validate_clone_isolation_evidence(
    target: DeploymentEnvironment,
    evidence: Mapping[str, Any] | None,
) -> dict[str, Any]:
    if not isinstance(evidence, Mapping):
        raise _error("clone restore requires host-service fence evidence")
    results = evidence.get("results") or ()
    expected_results = evidence.get("expected_host_command_count")
    if (
        evidence.get("schema") != "okg.environment_host_service_fence/v1"
        or evidence.get("ok") is not True
        or evidence.get("status") != "fenced"
        or evidence.get("environment_id") != target.id
        or evidence.get("descriptor_digest") != target.descriptor_digest
        or not isinstance(expected_results, int)
        or expected_results <= 0
        or len(results) != expected_results
        or any(
            int(row.get("returncode", 1)) != 0
            for row in results
        )
    ):
        raise _error("clone restore host-service fence evidence is invalid")
    try:
        captured = datetime.fromisoformat(str(evidence.get("captured_at") or ""))
    except ValueError as exc:
        raise _error("clone restore host-service fence timestamp is invalid") from exc
    if captured.tzinfo is None:
        raise _error("clone restore host-service fence timestamp lacks timezone")
    age = datetime.now(timezone.utc) - captured.astimezone(timezone.utc)
    if age < timedelta(0) or age > timedelta(minutes=5):
        raise _error(
            "clone restore host-service fence evidence is stale",
            age_seconds=age.total_seconds(),
        )
    return dict(evidence)


def _load_backup_report(path: str | Path) -> dict[str, Any]:
    target = Path(path).expanduser().resolve()
    try:
        raw = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise _error("clone backup report is unreadable", path=str(target)) from exc
    if not isinstance(raw, dict) or raw.get("schema") != CLONE_BACKUP_SCHEMA:
        raise _error("clone backup report has the wrong schema", path=str(target))
    source = raw.get("source")
    if (
        not isinstance(source, dict)
        or not isinstance(source.get("timescaledb_version"), str)
        or not source["timescaledb_version"]
    ):
        raise _error(
            "clone backup report does not bind a TimescaleDB source version",
            path=str(target),
        )
    return raw


def _class_counts(inventory: list[dict[str, Any]]) -> dict[str, Any]:
    tables = Counter(row["disposition"] for row in inventory)
    rows = Counter()
    for row in inventory:
        rows[row["disposition"]] += int(row["rows"])
    return {key: {"tables": tables[key], "rows": rows[key]} for key in sorted(tables)}


def _rows_for(inventory: list[dict[str, Any]], disposition: str) -> list[dict[str, Any]]:
    return [dict(row) for row in inventory if row["disposition"] == disposition]


def _regeneration_inventory(
    inventory: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows = {str(row["table"]): int(row["rows"]) for row in inventory}

    def total(*tables: str) -> int:
        return sum(rows.get(table, 0) for table in tables)

    dbos_schedule_rows = sum(
        count
        for table, count in rows.items()
        if table.startswith("dbos.") and "schedule" in table
    )
    return [
        {
            "class": "database_credentials",
            "status": "service_install",
            "rows": 0,
            "role_count": 3,
        },
        {
            "class": "nomos_baseline",
            "status": "first_publish",
            "rows": total(
                "okg.nomos_config_activations",
                "okg.nomos_config_sets",
                "okg.nomos_policy_activations",
                "okg.nomos_policy_bundles",
                "okg.nomos_decisions",
            ),
        },
        {
            "class": "source_progress",
            "status": "first_ingest",
            "rows": total("okg.source_progress", "okg.source_watermarks"),
        },
        {
            "class": "dbos_schedules",
            "status": "worker_boot",
            "rows": dbos_schedule_rows,
        },
        {
            "class": "mcp_registration",
            "status": "service_install",
            "rows": 0,
        },
        {
            "class": "readiness_evidence",
            "status": "post_publish",
            "rows": 0,
        },
    ]


def _clear_staging_runtime_authority(
    target: DeploymentEnvironment,
) -> list[str]:
    paths = [
        target.runtime.state_root / "worker-authority.json",
        target.runtime.state_root
        / "service-authority"
        / f"{target.runtime.worker_service}.json",
        target.runtime.state_root
        / "service-authority"
        / f"{target.ui.service}.json",
        target.runtime.state_root / "supervisor/mcp-registration.json",
        target.runtime.state_root / "supervisor/database-role-credentials.json",
        target.runtime.state_root / "supervisor/promotion-supervision.json",
        target.runtime.state_root / "supervisor/postgres-password",
        target.runtime.local_env_file,
        target.readiness.evidence_path,
    ]
    removed: list[str] = []
    for path in paths:
        if path.exists() or path.is_symlink():
            path.unlink()
            removed.append(str(path))
    return removed


def _recreate_database(dsn: str, *, expected_database: str) -> None:
    parsed = dsn_mod.parse(dsn)
    database = str(parsed.dbname or "")
    if database != expected_database or database in {"", "postgres", "template0", "template1"}:
        raise _error(
            "refusing to recreate an unexpected staging database",
            expected=expected_database,
            observed=database,
        )
    maintenance = parsed.with_dbname("postgres").render()
    with db_connect(maintenance, autocommit=True) as conn:
        conn.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE datname = %s AND pid <> pg_backend_pid()",
            (database,),
        )
        conn.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(database)))
        conn.execute(
            sql.SQL("CREATE DATABASE {} TEMPLATE template0").format(sql.Identifier(database))
        )
        conn.execute(
            sql.SQL(
                "ALTER DATABASE {} SET default_transaction_read_only = on"
            ).format(sql.Identifier(database))
        )
        conn.execute(
            sql.SQL("REVOKE CONNECT ON DATABASE {} FROM PUBLIC").format(
                sql.Identifier(database)
            )
        )
        for role in ("app_ro", "app_rw", "okg_mcp"):
            exists = conn.execute(
                "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = %s)",
                (role,),
            ).fetchone()[0]
            if exists:
                conn.execute(
                    sql.SQL("REVOKE CONNECT ON DATABASE {} FROM {}").format(
                        sql.Identifier(database),
                        sql.Identifier(role),
                    )
                )


def _writable_clone_dsn(dsn: str) -> str:
    """``dsn`` with read-only sessions switched off, appended to its own options."""
    return (
        dsn_mod.parse(dsn)
        .with_options("-c default_transaction_read_only=off")
        .render()
    )


def _clear_database_read_only_default(
    dsn: str,
    *,
    expected_database: str,
) -> None:
    parsed = dsn_mod.parse(dsn)
    database = str(parsed.dbname or "")
    if database != expected_database:
        raise _error(
            "refusing to reopen an unexpected staging database",
            expected=expected_database,
            observed=database,
        )
    maintenance = parsed.with_dbname("postgres").render()
    with db_connect(maintenance, autocommit=True) as conn:
        conn.execute(
            sql.SQL(
                "ALTER DATABASE {} RESET default_transaction_read_only"
            ).format(sql.Identifier(database))
        )


def _run(
    argv: list[str],
    *,
    operation: str,
    env: Mapping[str, str] | None = None,
) -> None:
    result = subprocess.run(
        argv,
        text=True,
        capture_output=True,
        check=False,
        env=dict(env) if env is not None else None,
    )
    if result.returncode != 0:
        raise _error(
            f"{operation} command failed",
            command=["<redacted-dsn>" if "://" in item else item for item in argv],
            returncode=result.returncode,
            stdout=result.stdout[-4000:],
            stderr=result.stderr[-4000:],
        )


def _tool(name: str) -> str:
    path = shutil.which(name)
    if path is None:
        raise _error("required clone tool is missing", tool=name)
    return path


def _libpq_command_target(dsn: str) -> tuple[str, dict[str, str]]:
    """The database name and the environment that points a libpq child at it.

    Every DSN parameter travels, ``options`` (``search_path``, timeouts) and
    ``passfile`` included; a parameter libpq cannot read from the environment
    refuses the clone instead of being dropped.
    """
    parsed = dsn_mod.parse(dsn)
    database = str(parsed.dbname or "")
    if not database:
        raise _error("database DSN does not name a database")
    try:
        child = parsed.to_libpq_env()
    except dsn_mod.DsnRenderError as exc:
        raise _error(
            "database DSN has parameters a pg_dump or pg_restore child cannot read",
            parameters=exc.data.get("keys", []),
        ) from exc
    env = dict(os.environ)
    env.update(child)
    return database, env


def _file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return f"sha256:{digest.hexdigest()}"


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True, default=str)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        temporary.chmod(0o600)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _error(message: str, **data: Any) -> DeploymentEnvironmentInvalid:
    return DeploymentEnvironmentInvalid(
        message,
        data=data,
        fix_hint="keep staging fenced and update the clone disposition contract",
    )
