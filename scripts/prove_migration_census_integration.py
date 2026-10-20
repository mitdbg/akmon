#!/usr/bin/env python3
"""Prove the migration census and a real publish on a disposable database."""

from __future__ import annotations

import argparse
from dataclasses import asdict
import json
import os
import re
import shlex
import subprocess
import sys
import uuid
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict
from sqlalchemy.engine import make_url


ROOT = Path(__file__).resolve().parents[1]
DATABASE_PREFIX = "okg_test_isolated_migration_census_"
REPORT_REFS = [
    "req.migration-census.integration",
    "test.migration-census.isolated-publish",
    "task.migration-census.integration",
]
TRANSFORM_GATE_CHECK_ID = (
    "transform.consistency.no_dangling+narrowing+cardinality.v3"
)
TRANSFORM_GATE_NAME = "transform_consistency_gate"


class PreexistingDatabaseError(RuntimeError):
    """The generated name was already owned before this proof attempted creation."""


def _is_nonbool_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _projection_receipt_is_exact(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False
    expected = {
        "nodes_added": 2,
        "nodes_retracted": 0,
        "nodes_updated": 0,
        "edges_added": 1,
        "edges_retracted": 0,
        "edges_updated": 0,
        "edges_projected_net": 1,
        "edges_checkpoint_visible_net": 1,
        "noop": False,
        "consistency_gate_mutations": {
            "dangling_edges_pruned": 0,
            "edges_removed": 0,
            "orphaned_derived_edges_retracted": 0,
        },
    }
    top_counters = (
        "nodes_added",
        "nodes_retracted",
        "nodes_updated",
        "edges_added",
        "edges_retracted",
        "edges_updated",
        "edges_projected_net",
        "edges_checkpoint_visible_net",
    )
    mutations = payload.get("consistency_gate_mutations")
    mutation_counters = (
        "dangling_edges_pruned",
        "edges_removed",
        "orphaned_derived_edges_retracted",
    )
    return (
        payload == expected
        and all(_is_nonbool_int(payload.get(key)) for key in top_counters)
        and payload.get("noop") is False
        and isinstance(mutations, dict)
        and all(_is_nonbool_int(mutations.get(key)) for key in mutation_counters)
    )


def _transform_consistency_is_clean(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False
    gates = payload.get("gates")
    counter_keys = (
        "gate_count",
        "blocked_gate_count",
        "degraded_gate_count",
        "open_observation_count",
        "newly_opened_count",
        "resolved_count",
    )
    if (
        not isinstance(gates, list)
        or not all(_is_nonbool_int(payload.get(key)) for key in counter_keys)
        or payload["gate_count"] != len(gates)
        or payload["gate_count"] < 1
        or payload["blocked_gate_count"] != 0
        or payload["degraded_gate_count"] != 0
        or payload["open_observation_count"] != 0
    ):
        return False
    matching = [
        gate
        for gate in gates
        if isinstance(gate, dict)
        and gate.get("check_id") == TRANSFORM_GATE_CHECK_ID
        and gate.get("gate_name") == TRANSFORM_GATE_NAME
    ]
    if len(matching) != 1:
        return False
    gate = matching[0]
    health = gate.get("health")
    return (
        gate.get("result") == "evaluated"
        and gate.get("blocks_publish") is False
        and gate.get("reason") == "no_violations"
        and _is_nonbool_int(gate.get("open_observation_count"))
        and gate["open_observation_count"] == 0
        and gate.get("coverage_complete") is True
        and gate.get("error") is None
        and isinstance(health, dict)
        and health.get("degraded") is False
    )


def _git(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )


def _head() -> str:
    proc = _git("rev-parse", "HEAD")
    if proc.returncode != 0:
        raise RuntimeError(f"cannot resolve git HEAD: {proc.stderr.strip()}")
    return proc.stdout.strip()


def _tree_changes() -> list[str]:
    proc = _git("status", "--porcelain=v1", "--untracked-files=all")
    if proc.returncode != 0:
        raise RuntimeError(f"cannot inspect git tree: {proc.stderr.strip()}")
    return [line for line in proc.stdout.splitlines() if line]


def _repo_relative(path: Path) -> str:
    resolved = path.resolve()
    try:
        return resolved.relative_to(ROOT).as_posix()
    except ValueError as exc:
        raise RuntimeError("report output must be inside the repository") from exc


def _maintenance_and_target_dsns(base_dsn: str, database: str) -> tuple[str, str]:
    info = conninfo_to_dict(base_dsn)
    base_database = str(info.get("dbname") or "")
    if "test" not in base_database and "isolated" not in base_database:
        raise RuntimeError(
            "OKG_TEST_DSN must name a non-production test or isolated database"
        )
    try:
        base_url = make_url(base_dsn)
    except Exception as exc:  # noqa: BLE001 - refuse non-URL DSNs before DBOS.
        raise RuntimeError("OKG_TEST_DSN must use a PostgreSQL URL") from exc
    maintenance = base_url.set(database="postgres").render_as_string(
        hide_password=False
    )
    target = base_url.set(database=database).render_as_string(hide_password=False)
    return maintenance, target


def _validate_generated_database_name(database: str) -> None:
    if not re.fullmatch(rf"{DATABASE_PREFIX}[0-9a-f]{{12}}", database):
        raise RuntimeError(f"refusing unexpected generated database name {database!r}")


def _create_database(maintenance_dsn: str, database: str) -> None:
    _validate_generated_database_name(database)
    with psycopg.connect(maintenance_dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = %s)",
                (database,),
            )
            if bool(cur.fetchone()[0]):
                raise PreexistingDatabaseError(
                    f"generated database already exists: {database}"
                )
            try:
                cur.execute(
                    sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database))
                )
            except psycopg.errors.DuplicateDatabase as exc:
                raise PreexistingDatabaseError(
                    f"generated database already exists: {database}"
                ) from exc


def _drop_database(maintenance_dsn: str, database: str) -> dict[str, Any]:
    _validate_generated_database_name(database)
    result: dict[str, Any] = {
        "attempted": True,
        "database": database,
        "drop_succeeded": False,
        "absent": False,
        "error": None,
    }
    try:
        with psycopg.connect(maintenance_dsn, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                    "WHERE datname = %s AND pid <> pg_backend_pid()",
                    (database,),
                )
                cur.execute(
                    sql.SQL("DROP DATABASE IF EXISTS {}").format(
                        sql.Identifier(database)
                    )
                )
                result["drop_succeeded"] = True
                cur.execute(
                    "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = %s)",
                    (database,),
                )
                result["absent"] = not bool(cur.fetchone()[0])
    except Exception as exc:  # noqa: BLE001 - teardown must become report data.
        result["error"] = f"{type(exc).__name__}: {exc}"
    return result


def _install_required_extensions(target_dsn: str) -> dict[str, list[str]]:
    """Install current substrate extensions in the newly created database."""
    from okg.substrate.db.migrate import ensure_required_extensions

    return ensure_required_extensions(target_dsn)


def _migrate_schema(target_dsn: str) -> dict[str, Any]:
    """Apply the current substrate schema and ordered migrations."""
    from okg.substrate.db.migrate import migrate

    return asdict(migrate(target_dsn))


def _run_json(argv: list[str], *, secrets: tuple[str, ...]) -> dict[str, Any]:
    proc = subprocess.run(
        argv,
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    try:
        payload = json.loads(proc.stdout)
    except (TypeError, ValueError):
        payload = None
    from okg.substrate.cli.deployment_readiness import (
        _ready_redact_doctor_value,
    )

    stderr = proc.stderr[-4000:]
    for secret in (*secrets, str(ROOT)):
        stderr = _ready_redact_doctor_value(stderr, secret)
    return {"returncode": proc.returncode, "payload": payload, "stderr": stderr}


def _run_doctor(argv: list[str], *, secrets: tuple[str, ...]) -> dict[str, Any]:
    """Run a verifier and retain raw truth separately from its persisted view."""
    from okg.substrate.cli.deployment_readiness import (
        _ready_redact_doctor_value,
    )

    result = _run_json(argv, secrets=secrets)
    safe_payload = result.get("payload")
    safe_stderr = result.get("stderr")
    for secret in (*secrets, str(ROOT)):
        safe_payload = _ready_redact_doctor_value(safe_payload, secret)
        safe_stderr = _ready_redact_doctor_value(safe_stderr, secret)
    return {
        **result,
        "safe_payload": safe_payload,
        "safe_stderr": safe_stderr,
    }


def _restore_safe_gate_identity(*, raw: Any, safe: Any) -> Any:
    """Restore only the reviewed non-secret v3 gate name after generic redaction."""
    if not isinstance(raw, dict) or not isinstance(safe, dict):
        return safe
    raw_consistency = raw.get("consistency")
    safe_consistency = safe.get("consistency")
    if not isinstance(raw_consistency, dict) or not isinstance(
        safe_consistency, dict
    ):
        return safe
    raw_gates = raw_consistency.get("gates")
    safe_gates = safe_consistency.get("gates")
    if not isinstance(raw_gates, list) or not isinstance(safe_gates, list):
        return safe
    for raw_gate, safe_gate in zip(raw_gates, safe_gates, strict=False):
        if not isinstance(raw_gate, dict) or not isinstance(safe_gate, dict):
            continue
        if (
            raw_gate.get("check_id") == TRANSFORM_GATE_CHECK_ID
            and raw_gate.get("gate_name") == TRANSFORM_GATE_NAME
        ):
            safe_gate["gate_name"] = TRANSFORM_GATE_NAME
            raw_health = raw_gate.get("health")
            safe_health = safe_gate.get("health")
            if (
                isinstance(raw_health, dict)
                and isinstance(safe_health, dict)
                and raw_health.get("gate_name") == TRANSFORM_GATE_NAME
            ):
                safe_health["gate_name"] = TRANSFORM_GATE_NAME
    return safe


def _doctor_view(run: dict[str, Any]) -> dict[str, Any]:
    raw_payload = run.get("payload")
    safe_payload = run.get("safe_payload")
    if not isinstance(safe_payload, dict):
        safe_payload = {}
    safe_stderr = run.get("safe_stderr")
    if not isinstance(safe_stderr, str):
        safe_stderr = "safe verification view unavailable"
    safe_payload = _restore_safe_gate_identity(raw=raw_payload, safe=safe_payload)
    return {
        "returncode": run.get("returncode"),
        "ok": safe_payload.get("ok"),
        "report": safe_payload,
        "stderr": safe_stderr,
    }


def _verification_commands(
    *, target_dsn: str, deployment: str, generation_id: str
) -> dict[str, list[str]]:
    base = [sys.executable, "-m", "okg.substrate.cli.main", "doctor"]
    return {
        "full_database_scope": [*base, "--dsn", target_dsn, "--json"],
        "database": [
            *base,
            "--check",
            "database",
            "--dsn",
            target_dsn,
            "--deployment",
            deployment,
            "--json",
        ],
        "generation_describe": [
            sys.executable,
            "-m",
            "okg.substrate.cli.main",
            "generation",
            "describe",
            generation_id,
            "--dsn",
            target_dsn,
            "--deployment",
            deployment,
            "--json",
        ],
    }


def _proof_view(run: dict[str, Any]) -> dict[str, Any]:
    payload = run.get("payload") if isinstance(run.get("payload"), dict) else {}
    surfaces = payload.get("operator_surfaces")
    if not isinstance(surfaces, dict):
        surfaces = {}
    doctor = surfaces.get("doctor")
    if not isinstance(doctor, dict):
        doctor = {}
    return {
        "returncode": run.get("returncode"),
        "ok": payload.get("ok"),
        "generation_id": payload.get("generation_id"),
        "compat_generation_id": payload.get("compat_generation_id"),
        "generation_status": payload.get("generation_status"),
        "delta": payload.get("delta"),
        "queue_dlq": payload.get("queue_dlq"),
        "deployment_doctor": {
            "returncode": doctor.get("returncode"),
            "overall_ok": doctor.get("overall_ok"),
            "errors": doctor.get("errors"),
        },
        "stderr": run.get("stderr"),
    }


def _conditions(
    *,
    deployment: str,
    manifest: dict[str, Any],
    helper: dict[str, Any],
    full_doctor: dict[str, Any],
    database_doctor: dict[str, Any],
    generation_describe: dict[str, Any],
    teardown: dict[str, Any],
) -> dict[str, bool]:
    manifest_payload = manifest.get("payload")
    helper_payload = helper.get("payload")
    helper_view = _proof_view(helper)
    full_doctor_payload = full_doctor.get("payload")
    database_doctor_payload = database_doctor.get("payload")
    generation_payload = generation_describe.get("payload")
    return {
        "manifest_returncode_zero": manifest.get("returncode") == 0,
        "manifest_ok": (
            isinstance(manifest_payload, dict) and manifest_payload.get("ok") is True
        ),
        "helper_returncode_zero": helper.get("returncode") == 0,
        "helper_ok": (
            isinstance(helper_payload, dict) and helper_payload.get("ok") is True
        ),
        "generation_published": helper_view["generation_status"] == "published",
        "generation_id_present": (
            isinstance(helper_view["generation_id"], str)
            and bool(helper_view["generation_id"].strip())
        ),
        "compat_generation_id_present": (
            _is_nonbool_int(helper_view["compat_generation_id"])
            and helper_view["compat_generation_id"] > 0
        ),
        "delta_exact": helper_view["delta"] == {"nodes": 2, "edges": 1},
        "queue_empty": (
            isinstance(helper_view["queue_dlq"], dict)
            and helper_view["queue_dlq"].get("active") == 0
            and helper_view["queue_dlq"].get("dlq") == 0
        ),
        "full_doctor_returncode_zero": full_doctor.get("returncode") == 0,
        "full_doctor_ok": (
            isinstance(full_doctor_payload, dict)
            and full_doctor_payload.get("ok") is True
        ),
        "database_doctor_returncode_zero": database_doctor.get("returncode") == 0,
        "database_doctor_ok": (
            isinstance(database_doctor_payload, dict)
            and database_doctor_payload.get("ok") is True
        ),
        "generation_describe_returncode_zero": (
            generation_describe.get("returncode") == 0
        ),
        "generation_describe_exact": (
            isinstance(generation_payload, dict)
            and generation_payload.get("generation_id")
            == helper_view["generation_id"]
            and generation_payload.get("deployment_name") == deployment
            and generation_payload.get("status") == "published"
            and generation_payload.get("target_generation_id")
            == helper_view["compat_generation_id"]
        ),
        "generation_counts_pinned": (
            isinstance(generation_payload, dict)
            and _is_nonbool_int(generation_payload.get("node_count"))
            and _is_nonbool_int(generation_payload.get("nodes_live_at_gen"))
            and _is_nonbool_int(generation_payload.get("edge_count"))
            and _is_nonbool_int(generation_payload.get("edges_live_at_gen"))
            and generation_payload["node_count"] >= 0
            and generation_payload["nodes_live_at_gen"] >= 0
            and generation_payload["edge_count"] >= 0
            and generation_payload["edges_live_at_gen"] >= 0
            and generation_payload["node_count"] == 2
            and generation_payload["edge_count"] == 1
            and generation_payload.get("node_count")
            == generation_payload.get("nodes_live_at_gen")
            and generation_payload.get("edge_count")
            == generation_payload.get("edges_live_at_gen")
        ),
        "generation_projection_receipt_exact": (
            isinstance(generation_payload, dict)
            and _projection_receipt_is_exact(
                generation_payload.get("projection_deltas")
            )
        ),
        "generation_gate_summary_clean": (
            isinstance(generation_payload, dict)
            and generation_payload.get("blocked_reason") is None
            and isinstance(generation_payload.get("invariant_summary"), dict)
            and generation_payload["invariant_summary"].get(
                "total_budget_exceeded"
            )
            is False
            and isinstance(
                generation_payload["invariant_summary"].get("cross_source"), dict
            )
            and generation_payload["invariant_summary"]["cross_source"].get(
                "failed"
            )
            == 0
            and generation_payload["invariant_summary"]["cross_source"].get(
                "timed_out"
            )
            == 0
            and isinstance(
                generation_payload["invariant_summary"].get("source_local"), dict
            )
            and all(
                isinstance(bucket, dict)
                and bucket.get("failed") == 0
                and bucket.get("timed_out") == 0
                for bucket in generation_payload["invariant_summary"][
                    "source_local"
                ].values()
            )
        ),
        "generation_consistency_clean": (
            isinstance(generation_payload, dict)
            and _transform_consistency_is_clean(
                generation_payload.get("consistency")
            )
        ),
        "database_drop_succeeded": teardown.get("drop_succeeded") is True,
        "database_absent": teardown.get("absent") is True,
    }


def _command(deployment_dir: Path, output_relative: str) -> str:
    deployment_relative = _repo_relative(deployment_dir)
    argv = [
        "uv",
        "run",
        "--extra",
        "postgres",
        "python",
        "scripts/prove_migration_census_integration.py",
        "--deployment-dir",
        deployment_relative,
        "--out",
        output_relative,
    ]
    return f"OKG_TEST_DSN=<redacted> {shlex.join(argv)}"


def _envelope(
    *,
    status: str,
    generation_ref: Any,
    command: str,
    artifact: str,
    data: dict[str, Any],
) -> dict[str, Any]:
    summary = (
        "PASS: manifest census and isolated +2 node/+1 edge publish are healthy"
        if status == "pass"
        else "FAIL: migration-census isolated proof did not satisfy every gate"
    )
    return {
        "evidence": [
            {
                "kind": "graph_report",
                "status": status,
                "refs": REPORT_REFS,
                "generation_ref": generation_ref,
                "command": command,
                "summary": summary,
                "artifact": artifact,
                "data": data,
            }
        ]
    }


def _write_report(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def run(*, base_dsn: str, deployment_dir: Path, output: Path) -> dict[str, Any]:
    output = output.resolve()
    output_relative = _repo_relative(output)
    command = _command(deployment_dir.resolve(), output_relative)
    if output.exists():
        raise RuntimeError(
            f"refusing to overwrite designated report: {output_relative}"
        )

    head_before = _head()
    tree_before = _tree_changes()
    if tree_before:
        raise RuntimeError(
            "exact-head proof requires a clean tracked and untracked tree: "
            + json.dumps(tree_before)
        )

    database = f"{DATABASE_PREFIX}{uuid.uuid4().hex[:12]}"
    maintenance_dsn, target_dsn = _maintenance_and_target_dsns(base_dsn, database)
    deployment = str(
        __import__("yaml")
        .safe_load((deployment_dir / "deployment.yaml").read_text())["name"]
    )
    create_attempted = False
    created = False
    manifest: dict[str, Any] = {
        "returncode": None,
        "payload": None,
        "stderr": "not run",
    }
    helper: dict[str, Any] = {
        "returncode": None,
        "payload": None,
        "stderr": "not run",
    }
    full_doctor: dict[str, Any] = {
        "returncode": None,
        "payload": None,
        "stderr": "not run",
    }
    database_doctor: dict[str, Any] = {
        "returncode": None,
        "payload": None,
        "stderr": "not run",
    }
    generation_describe: dict[str, Any] = {
        "returncode": None,
        "payload": None,
        "stderr": "not run",
    }
    extensions: dict[str, Any] = {
        "ok": False,
        "created": [],
        "already_present": [],
        "error": "not run",
    }
    migration: dict[str, Any] = {"ok": False, "result": None, "error": "not run"}
    teardown: dict[str, Any] = {
        "attempted": False,
        "database": database,
        "drop_succeeded": False,
        "absent": False,
        "error": "database was not created",
    }
    execution_error: str | None = None
    try:
        create_attempted = True
        _create_database(maintenance_dsn, database)
        created = True
        extension_result = _install_required_extensions(target_dsn)
        extensions = {"ok": True, "error": None, **extension_result}
        migration_result = _migrate_schema(target_dsn)
        migration = {"ok": True, "result": migration_result, "error": None}
        manifest = _run_json(
            [sys.executable, "scripts/check_schema_migration_manifest.py", "--json"],
            secrets=(base_dsn, target_dsn, maintenance_dsn),
        )
        helper = _run_json(
            [
                sys.executable,
                "scripts/prove_deployment_apply.py",
                "--dsn",
                target_dsn,
                "--deployment-dir",
                str(deployment_dir.resolve()),
                "--json",
            ],
            secrets=(base_dsn, target_dsn, maintenance_dsn),
        )
        helper_view = _proof_view(helper)
        if not (
            isinstance(helper_view["generation_id"], str)
            and helper_view["generation_id"].strip()
        ):
            raise RuntimeError("publish helper did not return a canonical generation ID")
        verification_commands = _verification_commands(
            target_dsn=target_dsn,
            deployment=deployment,
            generation_id=helper_view["generation_id"],
        )
        full_doctor = _run_doctor(
            verification_commands["full_database_scope"],
            secrets=(base_dsn, target_dsn, maintenance_dsn),
        )
        database_doctor = _run_doctor(
            verification_commands["database"],
            secrets=(base_dsn, target_dsn, maintenance_dsn),
        )
        generation_describe = _run_doctor(
            verification_commands["generation_describe"],
            secrets=(base_dsn, target_dsn, maintenance_dsn),
        )
    except PreexistingDatabaseError as exc:
        create_attempted = False
        execution_error = f"{type(exc).__name__}: {exc}"
    except Exception as exc:  # noqa: BLE001 - failure must be reportable.
        execution_error = f"{type(exc).__name__}: {exc}"
        if created and extensions.get("ok") is not True:
            extensions["error"] = execution_error
        elif created and migration.get("ok") is not True:
            migration["error"] = execution_error
    finally:
        if create_attempted:
            teardown = _drop_database(maintenance_dsn, database)

    conditions = _conditions(
        deployment=deployment,
        manifest=manifest,
        helper=helper,
        full_doctor=full_doctor,
        database_doctor=database_doctor,
        generation_describe=generation_describe,
        teardown=teardown,
    )
    data: dict[str, Any] = {
        "schema": "okg.pact.graph_report.migration_census_integration.v1",
        "git": {
            "head_before": head_before,
            "head_after": None,
            "unchanged": False,
            "tree_before": tree_before,
            "tree_after": None,
            "only_report_created": False,
        },
        "database": {
            "generated_name": database,
            "create_attempted": create_attempted,
            "created": created,
        },
        "extensions": extensions,
        "migration": migration,
        "manifest": {
            "returncode": manifest.get("returncode"),
            "ok": (
                manifest.get("payload", {}).get("ok")
                if isinstance(manifest.get("payload"), dict)
                else None
            ),
            "errors": (
                manifest.get("payload", {}).get("errors")
                if isinstance(manifest.get("payload"), dict)
                else None
            ),
            "stderr": manifest.get("stderr"),
        },
        "deployment_apply": _proof_view(helper),
        "doctors": {
            "full_database_scope": _doctor_view(full_doctor),
            "database": _doctor_view(database_doctor),
        },
        "generation_describe": _doctor_view(generation_describe),
        "teardown": teardown,
        "execution_error": execution_error,
        "conditions": conditions,
    }
    generation_ref = data["deployment_apply"].get("generation_id")
    conditions["required_extensions_ready"] = extensions.get("ok") is True
    conditions["schema_migrated"] = migration.get("ok") is True
    provisional = _envelope(
        status="fail",
        generation_ref=generation_ref,
        command=command,
        artifact=output_relative,
        data=data,
    )
    _write_report(output, provisional)

    head_after = _head()
    tree_after = _tree_changes()
    expected_change = f"?? {output_relative}"
    only_report_created = tree_after == [expected_change]
    data["git"].update(
        {
            "head_after": head_after,
            "unchanged": head_after == head_before,
            "tree_after": tree_after,
            "only_report_created": only_report_created,
        }
    )
    conditions["git_head_unchanged"] = head_after == head_before
    conditions["only_report_created"] = only_report_created
    passed = execution_error is None and all(conditions.values())
    final = _envelope(
        status="pass" if passed else "fail",
        generation_ref=generation_ref,
        command=command,
        artifact=output_relative,
        data=data,
    )
    _write_report(output, final)
    return final


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", default=os.environ.get("OKG_TEST_DSN"))
    # Required: the historical default, deployments/okg-ci, is a private
    # deployment that the public copy does not carry.
    parser.add_argument(
        "--deployment-dir",
        type=Path,
        required=True,
        help="deployment directory to prove against (for example deployments/okg-ci)",
    )
    parser.add_argument("--out", type=Path, required=True)
    return parser


def main() -> int:
    args = _parser().parse_args()
    if not args.dsn:
        raise SystemExit("Provide --dsn or set OKG_TEST_DSN")
    try:
        report = run(
            base_dsn=args.dsn,
            deployment_dir=args.deployment_dir,
            output=args.out,
        )
    except Exception as exc:  # preflight failures cannot safely write the artifact.
        try:
            artifact = _repo_relative(args.out)
            command = _command(args.deployment_dir.resolve(), artifact)
        except Exception:  # noqa: BLE001 - preserve the original refusal.
            artifact = args.out.as_posix()
            command = "migration-census integration proof preflight"
        report = _envelope(
            status="fail",
            generation_ref=None,
            command=command,
            artifact=artifact,
            data={
                "schema": "okg.pact.graph_report.migration_census_integration.v1",
                "preflight_error": f"{type(exc).__name__}: {exc}",
            },
        )
        print(json.dumps(report, indent=2, sort_keys=True))
        return 1
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["evidence"][0]["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
