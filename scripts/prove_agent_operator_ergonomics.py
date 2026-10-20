#!/usr/bin/env python3
"""Run the isolated live proof for issue 801's operator seams."""
from __future__ import annotations

import argparse
import contextlib
import io
import json
import os
import sys
import tempfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import psycopg
from psycopg.conninfo import conninfo_to_dict

from okg.substrate.cli.main import (
    _build_parser,
    _trace_graph_node_preview,
    main as cli_main,
)
from okg.substrate.db.migrate import migrate
from okg.substrate.exchange.harness import database_doctor, describe_generation
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.library.sources.pact import PACTSource
from okg.substrate.pact.models import Change, PactManifest
from okg.substrate.pact.store import save_manifest
from okg.substrate.publisher import Publisher
from okg.substrate.runtime import admission_workflows
from okg.substrate.runtime.services import cleanup_stale_pending_workflows
from scripts.proof_catalog_authority import bind_proof_catalog_writer


DEPLOYMENT = "agent-operator-ergonomics-proof"
DBOS_APP_NAME = "agent-operator-proof"
APPROVED_NODE = "observation:runtime-approved"
TRACE_NODE = "observation:trace-native"
TRACE_NEIGHBOR = "paper:trace-grounding"


def _assert_isolated(dsn: str, allow_non_test_dsn: bool) -> None:
    database = str(conninfo_to_dict(dsn).get("dbname") or "")
    if allow_non_test_dsn:
        return
    if "test" not in database and "isolated" not in database:
        raise RuntimeError(
            f"refusing operator proof against non-test database {database!r}"
        )


def _provision_dbos_runtime(dsn: str) -> None:
    from dbos import DBOS, DBOSConfig

    try:
        DBOS.destroy()
    except Exception:  # noqa: BLE001 - no singleton is the common case.
        pass
    DBOS(config=DBOSConfig(
        name=DBOS_APP_NAME,
        system_database_url=dsn,
        database_url=dsn,
        run_admin_server=False,
    ))
    try:
        DBOS.launch()
    finally:
        DBOS.destroy()


def _seed_catalog(dsn: str) -> int:
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        catalog_version = int(cur.fetchone()[0])
        for subtype in ("observation", "paper", "pact_task"):
            cur.execute(
                "INSERT INTO okg.subtypes (name, catalog_version_id) "
                "VALUES (%s, %s)",
                (subtype, catalog_version),
            )
            cur.execute(
                "INSERT INTO okg.subtype_ancestors "
                "(catalog_version_id, subtype_name, ancestor_name, depth) "
                "VALUES (%s, %s, %s, 0)",
                (catalog_version, subtype, subtype),
            )
        cur.execute(
            "INSERT INTO okg.narrowings "
            "(parent_subtype, edge_type, child_subtype, catalog_version_id) "
            "VALUES ('observation', 'grounded_in', 'paper', %s)",
            (catalog_version,),
        )
    return catalog_version


def _live_graph_counts(dsn: str) -> dict[str, int]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        graph = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
        ).checkout("default")
        nodes = int(
            graph.query("SELECT count(*) AS n FROM okg.graph_nodes")[0]["n"]
            or 0
        )
        edges = int(
            graph.query("SELECT count(*) AS n FROM okg.graph_edges")[0]["n"]
            or 0
        )
    return {"nodes_live": nodes, "edges_live": edges}


def _run_cli_json(argv: list[str]) -> dict[str, Any]:
    output = io.StringIO()
    with contextlib.redirect_stdout(output):
        exit_code = cli_main([*argv, "--json"])
    if exit_code != 0:
        raise RuntimeError(
            f"proof CLI command failed ({exit_code}): {' '.join(argv)}"
        )
    payload = json.loads(output.getvalue())
    if not isinstance(payload, dict):
        raise RuntimeError(f"proof CLI returned a non-object: {payload!r}")
    return payload


def _approver_config() -> dict[str, Any]:
    return {
        "approver": {
            "enabled": True,
            "actor": "runtime.approver",
            "rules": [{
                "id": "bounded.cooper-observation",
                "producer_ids": ["agent:cooper"],
                "producer_kinds": ["agent"],
                "intents": ["record_observation"],
                "node_subtypes": ["observation"],
                "edge_types": [],
                "max_nodes": 1,
                "max_edges": 1,
            }],
        },
    }


def _authority() -> dict[str, Any]:
    return {
        "producer_policy": {
            "producer_id": "agent:cooper",
            "producer_kind": "agent",
            "trust_label": "review-trusted",
            "admission_mode": "review",
            "allowed_record_kinds": ["node"],
            "output_signature": {
                "nodes": [{
                    "subtype": "observation",
                    "clause_id": "allow_observation",
                }],
            },
            "policy_clauses": [{
                "id": "allow_observation",
                "effect": "allow",
                "record_kind": "node",
                "subtype": "observation",
            }],
        },
    }


def _admission_counts(dsn: str) -> dict[str, int]:
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT (SELECT count(*) FROM okg.admission_events), "
            "(SELECT count(*) FROM okg.admission_record_refs), "
            "(SELECT count(*) FROM okg.fact_batch_receipts), "
            "(SELECT count(*) FROM okg.node_facts)"
        )
        events, refs, receipts, facts = cur.fetchone()
    return {
        "events": int(events),
        "record_refs": int(refs),
        "fact_receipts": int(receipts),
        "node_facts": int(facts),
    }


def _pact_task_facts() -> tuple[list[NodeFact], dict[str, str]]:
    prior_projection = os.environ.get("OKG_PACT_DECISION_PROJECTION")
    os.environ["OKG_PACT_DECISION_PROJECTION"] = "off"
    try:
        with tempfile.TemporaryDirectory(prefix="okg-pact-status-proof-") as raw:
            root = Path(raw)
            statuses = ["todo", "in_progress", "blocked", "done"]
            manifest = PactManifest(
                pact_version=4,
                target_pact_version=4,
                change=Change(id="status-proof", title="Status proof"),
                requirements=[{
                    "id": "req.status-proof",
                    "statement": "PACT task status projects exactly.",
                    "evidence_policy": {"required": ["executed_test"]},
                    "acceptance": [{
                        "id": "acc.status-proof",
                        "description": "All authored statuses survive.",
                        "evidence": ["executed_test"],
                    }],
                }],
                tasks=[{
                    "id": f"task.{status}",
                    "title": status,
                    "status": status,
                    "verification": "isolated proof",
                    "requirements": ["req.status-proof"],
                } for status in statuses],
                test_plan=[{
                    "id": "test.status-proof",
                    "requirement": "req.status-proof",
                    "test_id": "tests/pact/test_pact_source.py",
                }],
            )
            save_manifest(root, manifest)
            facts = list(PACTSource(
                repo="okg",
                pact_root=root / "pact",
            ).run("status-proof", mode="reconcile").facts)
            task_facts = [
                fact for fact in facts
                if isinstance(fact, NodeFact) and fact.subtype == "pact_task"
            ]
            projected = {
                fact.node_id: str(fact.attrs["status"])
                for fact in task_facts
            }
            return task_facts, projected
    finally:
        if prior_projection is None:
            os.environ.pop("OKG_PACT_DECISION_PROJECTION", None)
        else:
            os.environ["OKG_PACT_DECISION_PROJECTION"] = prior_projection


def _apply_chronos_native_fixture(dsn: str) -> dict[str, Any]:
    task_facts, projected_statuses = _pact_task_facts()
    run_id = str(uuid.uuid4())
    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        edit = store.apply_facts(
            [
                NodeFact(
                    TRACE_NODE,
                    "observation",
                    {
                        "text": "Chronos-native trace proof",
                        "minted_from": "alias:cooper-observation",
                        "nomos_source_lineage": ["cooper", "review"],
                        "producer_id": "agent:cooper",
                    },
                    {"fixture": "trace-native"},
                    {"revision": 1},
                ),
                NodeFact(
                    TRACE_NEIGHBOR,
                    "paper",
                    {"title": "Grounding paper"},
                    {"fixture": "trace-neighbor"},
                    {"revision": 1},
                ),
                EdgeFact(
                    TRACE_NODE,
                    TRACE_NEIGHBOR,
                    "grounded_in",
                    attrs={"enricher": "proof-grounder"},
                    provenance="enriched",
                    source_record_id={"fixture": "trace-edge"},
                    source_revision={"revision": 1},
                ),
                *task_facts,
            ],
            source="proof.chronos-native",
            run_id=run_id,
        )
    finally:
        store.close()
    return {
        "run_id": run_id,
        "nodes_upserted": edit.nodes_upserted,
        "edges_upserted": edit.edges_upserted,
        "projected_statuses": projected_statuses,
    }


def _runtime_approval(dsn: str) -> dict[str, Any]:
    original_resolver = admission_workflows._resolve_dsn
    admission_workflows._resolve_dsn = lambda _deployment: dsn
    try:
        kwargs = {
            "deployment": DEPLOYMENT,
            "payload": {
                "node": {
                    "node_id": APPROVED_NODE,
                    "subtype": "observation",
                    "attrs": {"text": "bounded approved observation"},
                },
            },
            "producer_id": "agent:cooper",
            "producer_kind": "agent",
            "intent": "record_observation",
            "idempotency_key": "cooper-runtime-approval-proof",
            "authority": _authority(),
            "check_summary": {"proof": "issue-801"},
            "approver_config": _approver_config(),
        }
        first = admission_workflows._run_submit_with_runtime_approver(**kwargs)
        before_replay = _admission_counts(dsn)
        replay = admission_workflows._run_submit_with_runtime_approver(**kwargs)
        after_replay = _admission_counts(dsn)
    finally:
        admission_workflows._resolve_dsn = original_resolver
    if first["status"] != "accepted" or replay["status"] != "accepted":
        raise RuntimeError(f"runtime approver did not accept: {first}, {replay}")
    if before_replay != after_replay:
        raise RuntimeError(
            f"runtime approver replay mutated state: {before_replay} -> "
            f"{after_replay}"
        )
    return {
        "first": first,
        "replay": replay,
        "counts_before_replay": before_replay,
        "counts_after_replay": after_replay,
    }


def _trigger_dedupe_proof(dsn: str) -> dict[str, Any]:
    """Exercise poll suppression against a real, active DBOS watch run."""
    from dbos import DBOS, DBOSConfig

    from okg.substrate.runtime import partition_queue
    from okg.substrate.runtime import source_sync_workflow as workflow_module
    from okg.substrate.runtime.partition_queue import (
        register_source_sync_partition_queue,
    )
    from okg.substrate.ingest.adapter_factory import construct_source_adapter
    from okg.substrate.ingest.runner import SourceRunner
    from okg.substrate.source_sync import SourceSyncEvent, SourceSyncJob
    from okg.substrate.source_sync_enqueue import enqueue_source_sync_via_dbos
    from okg.substrate.source_sync_producers import run_poll_once
    from okg.substrate.sources.registry import load_source_registry

    with tempfile.TemporaryDirectory(prefix="okg-trigger-dedupe-proof-") as raw:
        root = Path(raw)
        dep_dir = root / DEPLOYMENT
        dep_dir.mkdir()
        (dep_dir / "schemas").mkdir()
        watch_root = root / "watched"
        watch_root.mkdir()
        doc = watch_root / "doc.md"
        started = root / "source-started"
        release = root / "source-release"
        doc.write_text("before\n", encoding="utf-8")

        module_name = "operator_watch_proof_source"
        (dep_dir / f"{module_name}.py").write_text(
            "from pathlib import Path\n"
            "import hashlib\n"
            "import time\n"
            "from okg.substrate.library.sources.base import NodeFact, SourceRun\n\n"
            "from okg.substrate.library.sources.content_hash_probe import "
            "ContentHashProbe\n\n"
            "class OperatorWatchProofSource:\n"
            "    name = 'operator-watch-proof'\n"
            "    profile = 'discovery_crawl'\n\n"
            "    change_probe_kind = 'content_hash'\n"
            "    probe_short_circuit_safe = True\n\n"
            "    def __init__(self, *, doc_path, started_path, release_path):\n"
            "        self.doc_path = Path(doc_path)\n"
            "        self.started_path = Path(started_path)\n"
            "        self.release_path = Path(release_path)\n"
            "        self.change_probe = ContentHashProbe(\n"
            "            content_items=lambda: [('doc.md', self.doc_path)],\n"
            "            config={'doc_path': str(self.doc_path)},\n"
            "            emit_targets=[self.run],\n"
            "        )\n\n"
            "    def run(self, run_id, *, mode='scope_incomplete', "
            "cursor=None, sync_scope=None):\n"
            "        self.started_path.write_text(str(run_id), encoding='utf-8')\n"
            "        deadline = time.monotonic() + 20.0\n"
            "        while not self.release_path.exists():\n"
            "            if time.monotonic() >= deadline:\n"
            "                raise RuntimeError('proof source release timed out')\n"
            "            time.sleep(0.02)\n"
            "        captured = self.doc_path.read_bytes()\n"
            "        revision = captured.decode('utf-8').strip()\n"
            "        applied_raw_token = self.change_probe.token_from_item_hashes({\n"
            "            'doc.md': hashlib.sha256(captured).hexdigest(),\n"
            "        })\n"
            "        return SourceRun(\n"
            "            facts=[NodeFact(\n"
            "                'observation:watch-workflow-proof',\n"
            "                'observation',\n"
            "                {'text': revision, 'workflow_run_id': str(run_id)},\n"
            "                {'path': 'doc.md'},\n"
            "                {'revision': revision},\n"
            "            )],\n"
            "            completed_scope=(mode == 'scope_complete'),\n"
            "            next_cursor={\n"
            "                'revision': revision,\n"
            "                'actual_workflow_run_id': str(run_id),\n"
            "            },\n"
            "            applied_raw_token=applied_raw_token,\n"
            "            applied_token_scope='global',\n"
            "            applied_token_authority='exact',\n"
            "            run_mode=mode,\n"
            "        )\n",
            encoding="utf-8",
        )
        (dep_dir / "source_registry.yaml").write_text(
            "sources:\n"
            "  docs:\n"
            f"    module: {module_name}\n"
            "    class: OperatorWatchProofSource\n"
            f"    ownership_id: {DEPLOYMENT}.docs\n"
            "    params:\n"
            f"      doc_path: {json.dumps(str(doc))}\n"
            f"      started_path: {json.dumps(str(started))}\n"
            f"      release_path: {json.dumps(str(release))}\n"
            "    source_class: discovery_crawl\n"
            "    record_identity_kind: scoped_locator\n"
            "    record_identity_fields: [path]\n"
            "    source_revision_kind: content_hash\n"
            "    deletion_semantics: missing_from_completed_scope\n"
            "    publication_mode: published_generation\n"
            "    source_policy:\n"
            "      sensitivity: local_private\n"
            "      data_classification: private\n"
            "      credential_ref_policy: none\n"
            "      live_call_allowed: false\n"
            "      exportability: no_export\n"
            "      retention: source_defined\n"
            "      provenance: local_filesystem\n"
            "      privacy_obligations: [audit]\n"
            "      store_raw: true\n"
            "    sync:\n"
            "      watch:\n"
            f"        roots: [{json.dumps(str(watch_root))}]\n"
            "        include_globs: ['**/*']\n"
            "        exclude_globs: []\n"
            "      triggers: [fs_event, poll, manual, reconcile]\n"
            "      default_event_mode: scope_incomplete\n"
            "      reconcile_mode: scope_complete\n"
            "      deletes_require_reconcile: true\n"
            "      scope:\n"
            "        accepts: [path]\n"
            "        unsupported_fallback: scope_complete\n"
            "      dependents:\n"
            "        on_event: none\n"
            "        on_reconcile: closure\n"
            "      compaction:\n"
            "        window_seconds: 60\n"
            "        max_events: 100\n"
            "        max_scope_keys: 100\n"
            "      poll:\n"
            "        probe: change_probe\n"
            "        interval_seconds: 300\n",
            encoding="utf-8",
        )
        (dep_dir / "deployment.yaml").write_text(
            f"name: {DEPLOYMENT}\n"
            "description: isolated watch workflow proof\n"
            "schema_dir: schemas\n"
            "source_registry: source_registry.yaml\n"
            "modules: []\n"
            "nomos:\n"
            "  deployment_posture:\n"
            "    deployment_class: dogfood_workspace\n"
            "    read_principals: [human_operator, agent]\n"
            "    write_principals: [human_operator, source_adapter]\n"
            "    mcp_write_default: review\n"
            "    export_policy: internal_only\n"
            "    secret_handling: secret_ref_required\n"
            "    live_call_default: review\n"
            "    allowed_source_sensitivities: [local_private]\n"
            "    allowed_data_classifications: [private]\n"
            "    allowed_exportability: [no_export]\n"
            "    privacy_obligations: [audit]\n"
            "    readiness_profiles: [dogfood]\n",
            encoding="utf-8",
        )
        previous_deployments_dir = os.environ.get("OKG_DEPLOYMENTS_DIR")
        os.environ["OKG_DEPLOYMENTS_DIR"] = str(root)
        registry = load_source_registry(
            dep_dir / "source_registry.yaml",
            repo_root_override=root,
        )
        caps = registry["docs"].sync
        if caps is None:
            raise RuntimeError("proof source sync capabilities did not load")

        sys.modules.pop(module_name, None)
        sys.path.insert(0, str(dep_dir))
        source_identity = f"{DEPLOYMENT}.docs"
        with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO okg.source_watermarks "
                "(source, last_applied_rev, last_reconcile_at) "
                "VALUES (%s, '{}'::jsonb, now()) "
                "ON CONFLICT (source) DO UPDATE "
                "SET last_applied_rev = EXCLUDED.last_applied_rev, "
                "last_reconcile_at = EXCLUDED.last_reconcile_at",
                (source_identity,),
            )
        constructed = construct_source_adapter(registry["docs"], dsn=dsn)
        baseline = SourceRunner(
            dsn,
            source_name=source_identity,
            deployment_name=DEPLOYMENT,
            graph_branch=constructed.graph_branch,
            registry_binding_fingerprint=(
                constructed.registry_binding_fingerprint
            ),
        ).evaluate_poll(
            constructed.adapter,
            mode=caps.default_event_mode,
            reconcile_due=False,
        )
        if baseline.effective_token is None:
            raise RuntimeError("proof change probe produced no effective baseline")
        with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE okg.source_watermarks "
                "SET last_applied_rev = %s::jsonb WHERE source = %s",
                (
                    json.dumps({
                        "change_probe_token": baseline.effective_token,
                    }),
                    source_identity,
                ),
            )
            cur.execute(
                """
                INSERT INTO okg.source_progress
                  (deployment, source_name, key, value, updated_at)
                VALUES (
                  %s, 'docs', 'sync.poll.change_probe.fingerprint', %s,
                  now() - interval '5 minutes'
                )
                ON CONFLICT (deployment, source_name, key) DO UPDATE
                SET value = EXCLUDED.value,
                    updated_at = EXCLUDED.updated_at
                """,
                (DEPLOYMENT, baseline.revision_hint),
            )
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT last_applied_rev FROM okg.source_watermarks "
                "WHERE source = %s",
                (source_identity,),
            )
            cursor_before = cur.fetchone()[0]
        doc.write_text("after\n", encoding="utf-8")

        original_resolver = workflow_module._resolve_context_cached
        workflow_module._resolve_context_cached = lambda _deployment: (
            SimpleNamespace(sync_root=root, runtime=None),
            dsn,
            dep_dir,
        )
        partition_queue._REGISTERED.clear()
        workflow_module._DECORATED.clear()
        workflow_id: str | None = None
        active_poll: dict[str, Any] | None = None
        completed_poll: dict[str, Any] | None = None
        replay_poll: dict[str, Any] | None = None
        try:
            try:
                DBOS.destroy()
            except Exception:  # noqa: BLE001 - no singleton is expected.
                pass
            DBOS(config=DBOSConfig(
                name=f"{DBOS_APP_NAME}-watch",
                system_database_url=dsn,
                database_url=dsn,
                run_admin_server=False,
            ))
            register_source_sync_partition_queue(deployment=DEPLOYMENT)
            workflow_module.register_source_sync_workflow(
                deployment=DEPLOYMENT,
            )
            DBOS.launch()
            event = SourceSyncEvent(
                deployment=DEPLOYMENT,
                source_name="docs",
                trigger_kind="fs_event",
                scope_key=None,
                revision_hint="content-change:after",
                observed_at=datetime.now(timezone.utc).isoformat(),
                reason="proof:watch:modified",
            )
            workflow_id = enqueue_source_sync_via_dbos(
                deployment=DEPLOYMENT,
                dsn=dsn,
                job=SourceSyncJob(
                    deployment=DEPLOYMENT,
                    sources=("docs",),
                    run_mode=caps.default_event_mode,
                    events=(event,),
                    include_predecessors=False,
                    include_dependents=False,
                    dependent_fanout=caps.dependents.on_event,
                    publish=False,
                ),
                deduplication_id="proof:watch:docs:after",
            )
            deadline = time.monotonic() + 20.0
            while not started.exists() and time.monotonic() < deadline:
                time.sleep(0.05)
            if not started.exists():
                raise RuntimeError(
                    f"actual source-sync workflow did not start: {workflow_id}"
                )

            active_poll = run_poll_once(
                dsn,
                deployment=DEPLOYMENT,
                registry=registry,
                source="docs",
                publish=False,
            )

            release.write_text("release\n", encoding="utf-8")
            terminal_status: str | None = None
            deadline = time.monotonic() + 30.0
            while time.monotonic() < deadline:
                with psycopg.connect(dsn) as conn, conn.cursor() as cur:
                    cur.execute(
                        "SELECT status FROM dbos.workflow_status "
                        "WHERE workflow_uuid = %s",
                        (workflow_id,),
                    )
                    row = cur.fetchone()
                terminal_status = str(row[0]) if row else None
                if terminal_status in {
                    "SUCCESS",
                    "ERROR",
                    "CANCELLED",
                    "MAX_RECOVERY_ATTEMPTS_EXCEEDED",
                }:
                    break
                time.sleep(0.05)
            if terminal_status != "SUCCESS":
                raise RuntimeError(
                    "actual watch-backed source-sync workflow failed: "
                    f"workflow_id={workflow_id} status={terminal_status}"
                )

            completed_poll = run_poll_once(
                dsn,
                deployment=DEPLOYMENT,
                registry=registry,
                source="docs",
                publish=False,
            )
            with psycopg.connect(dsn) as conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT last_applied_rev, last_reconcile_at "
                    "FROM okg.source_watermarks WHERE source = %s",
                    (source_identity,),
                )
                cursor_after, last_reconcile_at = cur.fetchone()
                cur.execute(
                    "SELECT value::jsonb FROM okg.source_progress "
                    "WHERE deployment = %s AND source_name = %s "
                    "AND key = 'sync.last_run'",
                    (DEPLOYMENT, source_identity),
                )
                last_run = cur.fetchone()[0]
            replay_poll = run_poll_once(
                dsn,
                deployment=DEPLOYMENT,
                registry=registry,
                source="docs",
                publish=False,
            )
            with psycopg.connect(dsn) as conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT last_applied_rev FROM okg.source_watermarks "
                    "WHERE source = %s",
                    (source_identity,),
                )
                cursor_after_replay = cur.fetchone()[0]
                cur.execute(
                    "SELECT count(*), array_agg(status ORDER BY workflow_uuid) "
                    "FROM dbos.workflow_status WHERE name = 'okg_source_sync' "
                    "AND queue_name = %s",
                    (f"okg-{DEPLOYMENT}-source-sync",),
                )
                workflow_count, workflow_statuses = cur.fetchone()
        finally:
            if not release.exists():
                release.write_text("release after failure\n", encoding="utf-8")
            try:
                DBOS.destroy()
            except Exception:  # noqa: BLE001 - preserve the proof failure.
                pass
            workflow_module._resolve_context_cached = original_resolver
            if previous_deployments_dir is None:
                os.environ.pop("OKG_DEPLOYMENTS_DIR", None)
            else:
                os.environ["OKG_DEPLOYMENTS_DIR"] = previous_deployments_dir
            workflow_module._DECORATED.clear()
            partition_queue._REGISTERED.clear()
            sys.modules.pop(module_name, None)
            if str(dep_dir) in sys.path:
                sys.path.remove(str(dep_dir))

    watch_execution_count = int(workflow_count)
    active_result = ((active_poll or {}).get("results") or [{}])[0]
    completed_result = ((completed_poll or {}).get("results") or [{}])[0]
    replay_result = ((replay_poll or {}).get("results") or [{}])[0]
    report = {
        "watch_execution_count": watch_execution_count,
        "poll_enqueue_count": max(watch_execution_count - 1, 0),
        "total_execution_count": watch_execution_count,
        "workflow_id": workflow_id,
        "workflow_statuses": list(workflow_statuses or ()),
        "cursor_advance_count": (
            int(cursor_after != cursor_before)
            + int(cursor_after_replay != cursor_after)
        ),
        "active_poll": active_poll,
        "completed_poll": completed_poll,
        "replay_poll": replay_poll,
        "source_cursor_before": cursor_before,
        "source_cursor_after": cursor_after,
        "source_cursor_after_replay": cursor_after_replay,
        "source_probe_baseline": baseline.revision_hint,
        "source_last_reconcile_at": last_reconcile_at,
        "source_last_run": last_run,
    }
    if (
        report["total_execution_count"] != 1
        or report["poll_enqueue_count"] != 0
        or report["cursor_advance_count"] != 1
        or active_result.get("changed") is not True
        or (
            (active_result.get("enqueue_suppressed") or {}).get("reason")
            != "watch_backed_active_source_sync"
        )
        or completed_result.get("changed") is not False
        or "enqueue" in completed_result
        or replay_result.get("changed") is not False
        or "enqueue" in replay_result
        or cursor_after == cursor_before
        or cursor_after_replay != cursor_after
        or cursor_after.get("revision") != "after"
        or last_run.get("workflow_id") != workflow_id
    ):
        raise RuntimeError(f"watch/poll dedupe proof failed: {report}")
    return report


def _dbos_worker_restart_return_to_queue(dsn: str, workflow_id: str) -> None:
    """Exercise DBOS's queued-workflow startup recovery transition."""
    import sqlalchemy as sa
    from dbos._schemas import SCHEMA_PLACEHOLDER
    from dbos._sys_db import SystemDatabase

    engine = sa.create_engine(
        "postgresql+psycopg://",
        creator=lambda: psycopg.connect(dsn),
    )
    try:
        restart_db = SimpleNamespace(engine=engine.execution_options(
            schema_translate_map={SCHEMA_PLACEHOLDER: "dbos"},
        ))
        SystemDatabase.clear_queue_assignment(restart_db, workflow_id)
    finally:
        engine.dispose()


def _reaper_proof(
    dsn: str,
    *,
    completed_cursor_advance_count: int,
) -> dict[str, Any]:
    old_ms = int((time.time() - 7200) * 1000)
    workload_shape = {
        "batch_size": 64,
        "samples_per_item": 3,
        "total_samples": 64 * 3,
    }
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")
        cur.execute("CREATE SCHEMA dbos")
        cur.execute(
            """
            CREATE TABLE dbos.workflow_status (
              workflow_uuid text PRIMARY KEY,
              status text NOT NULL,
              name text,
              queue_name text,
              application_version text,
              priority integer,
              created_at bigint NOT NULL,
              updated_at bigint NOT NULL,
              started_at_epoch_ms bigint,
              recovery_attempts integer,
              deduplication_id text,
              inputs text
            )
            """
        )
        cur.execute(
            """
            INSERT INTO dbos.workflow_status (
              workflow_uuid, status, name, queue_name, application_version,
              priority, created_at, updated_at, started_at_epoch_ms,
              recovery_attempts, deduplication_id, inputs
            ) VALUES
              ('wf-started-long', 'PENDING', 'okg_source_sync', %s,
               'proof', 100, %s, %s, %s, 0, 'sync:started:64x3', %s),
              ('wf-never-started', 'PENDING', 'okg_source_sync', %s,
               'proof', 100, %s, %s, NULL, 1, 'sync:wakeup', NULL)
            """,
            (
                f"okg-{DEPLOYMENT}-source-sync",
                old_ms,
                old_ms,
                old_ms,
                json.dumps({"proof_workload": workload_shape}),
                f"okg-{DEPLOYMENT}-source-sync",
                old_ms,
                old_ms,
            ),
        )
    boot_cleanup = cleanup_stale_pending_workflows(
        dsn=dsn,
        deployment=DEPLOYMENT,
        stale_after_seconds=600,
        recovered_wakeup_stale_after_seconds=300,
        cancel_started_pending=False,
    )
    maintenance_cleanup = cleanup_stale_pending_workflows(
        dsn=dsn,
        deployment=DEPLOYMENT,
        stale_after_seconds=600,
        recovered_wakeup_stale_after_seconds=300,
        cancel_started_pending=False,
    )
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT workflow_uuid, status FROM dbos.workflow_status "
            "ORDER BY workflow_uuid"
        )
        after_cleanup = dict(cur.fetchall())
    dry_run = _run_cli_json([
        "db",
        "dbos-gc",
        "--dsn",
        dsn,
        "--stale-after-seconds",
        "600",
        "--queue-name",
        f"okg-{DEPLOYMENT}-source-sync",
        "--workflow-name",
        "okg_source_sync",
        "--status",
        "PENDING",
    ])
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT workflow_uuid, status FROM dbos.workflow_status "
            "ORDER BY workflow_uuid"
        )
        after_dry_run = dict(cur.fetchall())
    if after_cleanup["wf-started-long"] != "PENDING":
        raise RuntimeError("automatic cleanup cancelled started work")
    if after_cleanup["wf-never-started"] != "CANCELLED":
        raise RuntimeError("automatic cleanup retained stale wakeup")
    if after_dry_run != after_cleanup:
        raise RuntimeError("dry-run DBOS GC mutated workflow state")
    _dbos_worker_restart_return_to_queue(dsn, "wf-started-long")
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT workflow_uuid, status FROM dbos.workflow_status "
            "ORDER BY workflow_uuid"
        )
        after_termination_confirmation = dict(cur.fetchall())
    if after_termination_confirmation["wf-started-long"] != "ENQUEUED":
        raise RuntimeError("worker restart did not confirm execution termination")
    confirmed = _run_cli_json([
        "db",
        "dbos-gc",
        "--dsn",
        dsn,
        "--stale-after-seconds",
        "600",
        "--queue-name",
        f"okg-{DEPLOYMENT}-source-sync",
        "--workflow-name",
        "okg_source_sync",
        "--status",
        "ENQUEUED",
        "--confirm",
        "--apply",
    ])
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT workflow_uuid, status FROM dbos.workflow_status "
            "ORDER BY workflow_uuid"
        )
        after_confirm = dict(cur.fetchall())
    if after_confirm["wf-started-long"] != "CANCELLED":
        raise RuntimeError("confirmed operator GC retained abandoned work")
    if completed_cursor_advance_count != 1:
        raise RuntimeError("completed source work did not advance cursor once")
    return {
        "workload_shape": workload_shape,
        "boot_cleanup": boot_cleanup,
        "maintenance_cleanup": maintenance_cleanup,
        "after_cleanup": after_cleanup,
        "operator_dry_run": dry_run,
        "after_dry_run": after_dry_run,
        "after_termination_confirmation": after_termination_confirmation,
        "operator_confirmed": confirmed,
        "after_confirm": after_confirm,
        "completed_cursor_advance_count": completed_cursor_advance_count,
    }


def _queue_health(dsn: str) -> dict[str, Any]:
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FILTER (WHERE status IN "
            "('PENDING','ENQUEUED','DELAYED')), "
            "count(*) FILTER (WHERE status IN ('ERROR','FAILED')) "
            "FROM dbos.workflow_status"
        )
        active, dlq = cur.fetchone()
    return {"available": True, "active": int(active), "dlq": int(dlq)}


def _isolated_doctor(dsn: str) -> dict[str, Any]:
    from okg.substrate.db import migrate as migrate_module

    original = dict(migrate_module.REQUIRED_EXTENSIONS)
    migrate_module.REQUIRED_EXTENSIONS.pop("pg_cron", None)
    try:
        return database_doctor(dsn)
    finally:
        migrate_module.REQUIRED_EXTENSIONS.clear()
        migrate_module.REQUIRED_EXTENSIONS.update(original)


def run(*, dsn: str, allow_non_test_dsn: bool = False) -> dict[str, Any]:
    _assert_isolated(dsn, allow_non_test_dsn)
    migrate(dsn)
    _provision_dbos_runtime(dsn)
    catalog_version = _seed_catalog(dsn)
    catalog_identity = bind_proof_catalog_writer(
        dsn,
        deployment_name=DEPLOYMENT,
        catalog_version_id=catalog_version,
        proof_name="agent-operator-ergonomics",
    )
    graph_counts_before = _live_graph_counts(dsn)
    approval = _runtime_approval(dsn)
    native = _apply_chronos_native_fixture(dsn)
    trigger_dedupe = _trigger_dedupe_proof(dsn)
    published = Publisher(
        dsn,
        DEPLOYMENT,
        enrichers=[],
        catalog_identity=catalog_identity,
    ).cycle()
    if published.get("status") != "published":
        raise RuntimeError(f"proof publish blocked: {published}")
    generation_id = str(published["generation_id"])
    graph_counts_after = _live_graph_counts(dsn)
    graph_row_deltas = {
        "nodes_live": (
            graph_counts_after["nodes_live"]
            - graph_counts_before["nodes_live"]
        ),
        "edges_live": (
            graph_counts_after["edges_live"]
            - graph_counts_before["edges_live"]
        ),
    }
    published_generation = describe_generation(dsn, generation_id)
    if (
        graph_row_deltas["nodes_live"] <= 0
        or graph_row_deltas["edges_live"] <= 0
        or graph_counts_after["nodes_live"]
        != int(published_generation["nodes_live"])
        or graph_counts_after["edges_live"]
        != int(published_generation["edges_live"])
    ):
        raise RuntimeError(
            "published graph counts did not show the expected positive delta: "
            f"before={graph_counts_before} after={graph_counts_after} "
            f"generation={published_generation}"
        )
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        trace = _trace_graph_node_preview(
            conn,
            TRACE_NODE,
            generation_id=generation_id,
        )
        cur.execute(
            "SELECT count(*) FROM okg.node_facts WHERE node_id = %s",
            (TRACE_NODE,),
        )
        trace_fact_rows = int(cur.fetchone()[0])
    if not trace["present"] or "replacement_command" in trace:
        raise RuntimeError(f"Chronos trace was not narrated: {trace}")
    if trace_fact_rows != 0:
        raise RuntimeError("Chronos-native trace fixture unexpectedly has facts")
    if [step["kind"] for step in trace["derivation_story"]] != [
        "node_origin",
        "source_lineage",
        "adjacent_edge",
    ]:
        raise RuntimeError(f"unexpected derivation order: {trace}")
    parser = _build_parser()
    trailing_json = parser.parse_args([
        "search",
        "--deployment",
        DEPLOYMENT,
        "--query",
        "cooper",
        "--json",
    ])
    reaper = _reaper_proof(
        dsn,
        completed_cursor_advance_count=int(
            trigger_dedupe["cursor_advance_count"]
        ),
    )
    doctor = _isolated_doctor(dsn)
    if not doctor["ok"]:
        raise RuntimeError(f"isolated database doctor failed: {doctor}")
    return {
        "deployment": DEPLOYMENT,
        "catalog_version_id": catalog_version,
        "published_generation": published_generation,
        "graph_row_counts_before": graph_counts_before,
        "graph_row_counts_after": graph_counts_after,
        "graph_row_deltas": graph_row_deltas,
        "runtime_approval": approval,
        "chronos_native_apply": native,
        "trace": trace,
        "trace_node_fact_rows": trace_fact_rows,
        "search_trailing_json": bool(trailing_json.json),
        "trigger_dedupe": trigger_dedupe,
        "reaper": reaper,
        "queue_health": _queue_health(dsn),
        "doctor": doctor,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", required=True)
    parser.add_argument("--out", type=Path)
    parser.add_argument("--allow-non-test-dsn", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    report = run(
        dsn=args.dsn,
        allow_non_test_dsn=args.allow_non_test_dsn,
    )
    encoded = json.dumps(report, indent=2, default=str) + "\n"
    if args.out is not None:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(encoded, encoding="utf-8")
    if args.json or args.out is None:
        print(encoded, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
