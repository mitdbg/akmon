#!/usr/bin/env python3
"""Run an isolated five-stage deployment-apply proof with a real publish."""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import uuid
from pathlib import Path
from typing import Any

import psycopg
from psycopg.conninfo import conninfo_to_dict

from okg.substrate.catalog.modules_compose import compose_catalog
from okg.substrate.deployment_apply import (
    ApplyCommandResult,
    apply_deployment_plan,
    deployment_apply_health,
    plan_deployment_apply,
    record_worker_acknowledgment,
)
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.publisher import Publisher
from okg.substrate.publisher.deployment_stack import (
    build_deployment_publisher_stack,
    build_runtime_catalog_identity,
)


def _proof_service_install(
    argv: list[str],
    env: dict[str, str],
    *,
    deployment: str,
    adapter: str,
    dsn: str,
    working_directory: Path,
) -> ApplyCommandResult:
    """Simulate the isolated proof's supervisor-definition write."""
    expected_argv = [
        "uv", "run", "okg", "dogfood", "service", "install",
        "--deployment", deployment,
        "--adapter", adapter,
        "--repo-root", str(working_directory.resolve()),
        "--dsn", "${OKG_DEPLOYMENT_APPLY_DSN}",
        "--apply", "--json",
    ]
    if argv != expected_argv or env != {"OKG_DEPLOYMENT_APPLY_DSN": dsn}:
        return ApplyCommandResult(2, "", "invalid isolated service-install command")
    destination = f"isolated-proof://{adapter}/{deployment}"
    return ApplyCommandResult(
        0,
        json.dumps(
            {
                "schema": "okg.dogfood.service_adapter.v1",
                "ok": True,
                "deployment": deployment,
                "adapter": adapter,
                "destination": destination,
                "written": destination,
                "content_sha256": "sha256:isolated-proof-no-host-write",
            }
        ),
    )


def _provision_dbos_runtime(dsn: str, deployment: str) -> None:
    """Install the real DBOS system schema so queue proof is authoritative."""
    from dbos import DBOS, DBOSConfig

    try:
        DBOS.destroy()
    except Exception:  # noqa: BLE001 - no prior singleton is the common case.
        pass
    DBOS(config=DBOSConfig(
        name=f"deployment-apply-proof-{deployment}",
        system_database_url=dsn,
        database_url=dsn,
        run_admin_server=False,
    ))
    try:
        DBOS.launch()
    finally:
        DBOS.destroy()


def _operator_surface(
    argv: list[str],
    *,
    env: dict[str, str],
    key: str,
    redaction_secrets: tuple[str, ...],
) -> dict[str, Any]:
    proc = subprocess.run(
        argv,
        check=False,
        capture_output=True,
        text=True,
        env={**os.environ, **env},
    )
    try:
        payload = json.loads(proc.stdout)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(
            f"operator surface did not emit JSON: {argv}: {proc.stderr[-1000:]}"
        ) from exc
    from okg.substrate.cli.deployment_readiness import (
        _ready_redact_doctor_value,
    )

    errors = payload.get("errors")
    for secret in redaction_secrets:
        errors = _ready_redact_doctor_value(errors, secret)
    return {
        "returncode": proc.returncode,
        "overall_ok": bool(payload.get("ok")),
        "deployment_apply": payload.get(key),
        "errors": errors,
    }


def _operator_surfaces(
    *, deployment: str, dsn: str, env: dict[str, str],
) -> dict[str, dict[str, Any]]:
    return {
        "readiness": _operator_surface(
            [
                "uv", "run", "okg", "deployment", "ready", deployment,
                "--checks", "ci", "--skip-doctor", "--json",
            ],
            env=env,
            key="deployment_apply",
            redaction_secrets=(dsn, str(Path(__file__).resolve().parents[1])),
        ),
        "dogfood": _operator_surface(
            [
                "uv", "run", "okg", "dogfood", "status",
                "--deployment", deployment, "--skip-doctor", "--json",
            ],
            env=env,
            key="deployment_apply",
            redaction_secrets=(dsn, str(Path(__file__).resolve().parents[1])),
        ),
        "doctor": _operator_surface(
            [
                "uv", "run", "okg", "doctor", "--dsn", dsn,
                "--deployment", deployment, "--json",
            ],
            env=env,
            key="deployment_apply",
            redaction_secrets=(dsn, str(Path(__file__).resolve().parents[1])),
        ),
    }


def _assert_isolated(dsn: str, allow_non_test_dsn: bool) -> None:
    database = str(conninfo_to_dict(dsn).get("dbname") or "")
    if allow_non_test_dsn:
        return
    if "test" not in database and "isolated" not in database:
        raise RuntimeError(
            f"refusing deployment-apply proof against non-test database {database!r}"
        )


def _counts(dsn: str, prefix: str) -> dict[str, int]:
    store = ChronosGraphStore.connect(dsn)
    try:
        graph = store.checkout("default")
        nodes = list(
            graph.query(
                "SELECT node_id FROM okg.graph_nodes WHERE node_id LIKE :prefix",
                {"prefix": f"{prefix}%"},
            )
        )
        edges = list(
            graph.query(
                "SELECT edge_id FROM okg.graph_edges WHERE src LIKE :prefix",
                {"prefix": f"{prefix}%"},
            )
        )
    finally:
        store.close()
    return {"nodes": len(nodes), "edges": len(edges)}


def _queue_counts(dsn: str, deployment: str) -> dict[str, Any]:
    from okg.substrate.runtime.partition_queue import source_sync_queue_name

    queue_name = source_sync_queue_name(deployment)
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT to_regclass('dbos.workflow_status')")
        if cur.fetchone()[0] is not None:
            cur.execute(
                """
                SELECT count(*) FILTER (
                         WHERE status IN ('PENDING','ENQUEUED','DELAYED')
                       ),
                       count(*) FILTER (
                         WHERE status IN (
                           'ERROR','FAILED','MAX_RECOVERY_ATTEMPTS',
                           'MAX_RECOVERY_ATTEMPTS_EXCEEDED'
                         )
                       )
                  FROM dbos.workflow_status
                 WHERE name = 'okg_source_sync' AND queue_name = %s
                """
                ,
                (queue_name,),
            )
            active, dlq = cur.fetchone()
            return {
                "source": "dbos.workflow_status",
                "available": True,
                "queue_name": queue_name,
                "active": int(active),
                "dlq": int(dlq),
            }
        return {
            "source": "dbos.workflow_status",
            "available": False,
            "queue_name": queue_name,
            "active": None,
            "dlq": None,
            "reason": "dbos.workflow_status is not installed",
        }


def _publish_with_deployment_stack(
    *, dsn: str, deployment: str, deployment_dir: Path
) -> dict[str, Any]:
    """Publish through the same deployment-owned stack as current runtime paths."""
    stack = build_deployment_publisher_stack(
        dsn=dsn,
        deployment_name=deployment,
        deployment_dir=deployment_dir,
    )
    catalog_identity = build_runtime_catalog_identity(
        dsn=dsn,
        deployment_name=deployment,
        deployment_dir=deployment_dir,
        command="migration census isolated proof",
    )
    return Publisher(
        dsn,
        deployment,
        catalog_identity=catalog_identity,
        **stack.publisher_kwargs(),
    ).cycle()


def run(
    *, dsn: str, deployment_dir: Path, allow_non_test_dsn: bool = False,
) -> dict[str, Any]:
    _assert_isolated(dsn, allow_non_test_dsn)
    raw = __import__("yaml").safe_load(
        (deployment_dir / "deployment.yaml").read_text()
    )
    deployment = str(raw["name"])
    _provision_dbos_runtime(dsn, deployment)
    token = uuid.uuid4().hex[:12]
    prefix = f"deployment-apply-proof:{token}:"
    composed = compose_catalog(deployment_dir)
    narrowing = next(iter(composed.narrowings))
    source_id = f"{prefix}source"
    target_id = f"{prefix}target"
    before = _counts(dsn, prefix)
    surface_env = {"OKG_CI_DSN": dsn}
    operator_surfaces_before = _operator_surfaces(
        deployment=deployment,
        dsn=dsn,
        env=surface_env,
    )
    dry_run = plan_deployment_apply(
        deployment=deployment, deployment_dir=deployment_dir, dsn=dsn,
    )
    published: dict[str, Any] = {}
    emitted = False

    def runner(argv: list[str], _env: dict[str, str]) -> ApplyCommandResult:
        nonlocal emitted, published
        if argv[3:5] == ["jobs", "enqueue-source"]:
            proc = subprocess.run(
                argv,
                check=False,
                capture_output=True,
                text=True,
                env={**os.environ, "OKG_CI_DSN": dsn},
            )
            result = ApplyCommandResult(proc.returncode, proc.stdout, proc.stderr)
            if result.returncode != 0:
                return result
            enqueue = result.json_payload()
            workflow_id = str((enqueue or {}).get("dbos_workflow_id") or "")
            if not emitted:
                with psycopg.connect(dsn, autocommit=True) as conn:
                    store = ChronosGraphStore.from_connection(conn, ensure_schema=True)
                    store.apply_facts(
                        [
                            NodeFact(source_id, narrowing.parent_subtype, {"proof": token}, {"id": source_id}, {"v": 1}),
                            NodeFact(target_id, narrowing.child_subtype, {"proof": token}, {"id": target_id}, {"v": 1}),
                            EdgeFact(
                                src=source_id,
                                dst=target_id,
                                edge_type=narrowing.edge_archetype,
                                attrs={"proof": token},
                                source_record_id={"id": token},
                                source_revision={"v": 1},
                            ),
                        ],
                        source="deployment-apply-proof",
                        strict_endpoints=False,
                    )
                published = _publish_with_deployment_stack(
                    dsn=dsn,
                    deployment=deployment,
                    deployment_dir=deployment_dir,
                )
                if published.get("status") != "published":
                    raise RuntimeError(f"proof publish blocked: {published}")
                emitted = True
            with psycopg.connect(dsn) as conn, conn.cursor() as cur:
                cur.execute(
                    "UPDATE dbos.workflow_status SET status = 'SUCCESS' "
                    "WHERE workflow_uuid = %s",
                    (workflow_id,),
                )
                if cur.rowcount != 1:
                    raise RuntimeError(
                        f"proof could not finalize DBOS workflow {workflow_id!r}"
                    )
                conn.commit()
            return result
        if argv[3:6] == ["dogfood", "service", "install"]:
            service_authority = dry_run["desired_state"]["service_authority"]
            return _proof_service_install(
                argv,
                _env,
                deployment=deployment,
                adapter=service_authority["adapter"],
                dsn=dsn,
                working_directory=Path(service_authority["working_directory"]),
            )
        if argv[3:5] == ["dogfood", "restart"]:
            desired = dry_run["desired_state"]
            record_worker_acknowledgment(
                dsn=dsn,
                deployment=deployment,
                deployment_dir=deployment_dir,
                active_sources=list(desired["source_names"]),
                active_schedules=list(desired["source_schedule_ids"]),
            )
            return ApplyCommandResult(0, json.dumps({"ok": True, "adapter": "isolated-proof"}))
        return ApplyCommandResult(2, "", f"unexpected command: {argv}")

    applied = apply_deployment_plan(
        deployment=deployment,
        deployment_dir=deployment_dir,
        dsn=dsn,
        confirm=dry_run["plan_id"],
        command_runner=runner,
        worker_ack_timeout_seconds=2,
    )
    replay_plan = plan_deployment_apply(
        deployment=deployment, deployment_dir=deployment_dir, dsn=dsn,
    )
    replay = apply_deployment_plan(
        deployment=deployment,
        deployment_dir=deployment_dir,
        dsn=dsn,
        confirm=replay_plan["plan_id"],
        command_runner=runner,
    )
    after = _counts(dsn, prefix)
    health = deployment_apply_health(
        deployment=deployment, deployment_dir=deployment_dir, dsn=dsn,
    )
    queue = _queue_counts(dsn, deployment)
    operator_surfaces = _operator_surfaces(
        deployment=deployment,
        dsn=dsn,
        env=surface_env,
    )
    ok = bool(
        applied.get("applied")
        and replay.get("write_free")
        and published.get("status") == "published"
        and after["nodes"] - before["nodes"] == 2
        and after["edges"] - before["edges"] == 1
        and health.get("ok")
        and queue.get("available") is True
        and queue.get("source") == "dbos.workflow_status"
        and queue.get("active") == 0
        and queue.get("dlq") == 0
        and all(
            (row.get("deployment_apply") or {}).get("ok") is False
            and (row.get("deployment_apply") or {}).get("status") == "blocked"
            for row in operator_surfaces_before.values()
        )
        and all(
            (row.get("deployment_apply") or {}).get("ok") is True
            for row in operator_surfaces.values()
        )
    )
    return {
        "schema": "okg.deployment_apply_proof/v1",
        "ok": ok,
        "deployment": deployment,
        "plan_id": dry_run["plan_id"],
        "desired_state_digest": dry_run["desired_state"]["desired_state_digest"],
        "ordered_actions": dry_run["actions"],
        "completed_stages": applied.get("completed_stages"),
        "generation_id": published.get("generation_id"),
        "compat_generation_id": published.get("compat_generation_id"),
        "generation_status": published.get("status"),
        "before": before,
        "after": after,
        "delta": {"nodes": after["nodes"] - before["nodes"], "edges": after["edges"] - before["edges"]},
        "queue_dlq": queue,
        "apply_health": health,
        "operator_surfaces_before": operator_surfaces_before,
        "operator_surfaces": operator_surfaces,
        "replay_write_free": replay.get("write_free", False),
        "replay_no_op": replay.get("no_op", False),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", required=True)
    # Required: the historical default, deployments/okg-ci, is a private
    # deployment that the public copy does not carry.
    parser.add_argument(
        "--deployment-dir",
        type=Path,
        required=True,
        help="deployment directory to prove against (for example deployments/okg-ci)",
    )
    parser.add_argument("--allow-non-test-dsn", action="store_true")
    parser.add_argument("--out", type=Path)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    result = run(
        dsn=args.dsn,
        deployment_dir=args.deployment_dir.resolve(),
        allow_non_test_dsn=args.allow_non_test_dsn,
    )
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(result, indent=2, default=str) + "\n")
    if args.json or not args.out:
        print(json.dumps(result, indent=2, default=str))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
