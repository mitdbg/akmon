#!/usr/bin/env python3
"""Prove strict library configuration admission on a disposable database."""

from __future__ import annotations

import argparse
from dataclasses import asdict
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Callable
from urllib.parse import quote, quote_plus

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict
from sqlalchemy.engine import make_url
import yaml

from okg.substrate import fastyaml


ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = ROOT / "src/okg/substrate/library/templates/docs-corpus"
DATABASE_PREFIX = "okg_test_isolated_library_config_core_"
REPORT_REFS = [
    "req.library-config-core.integration",
    "test.library-config-core.isolated-publish",
    "task.library-config-core.integration",
]
EXPECTED_PRODUCTION_BINDINGS = {
    "confluence": {
        "module": "okg.substrate.library.sources.confluence",
        "class": "ConfluenceSource",
        "configured_profile": "mutable_api",
        "frozen_profile": "mutable_api",
    },
    "docsite": {
        "module": "okg.substrate.library.sources.docsite",
        "class": "DocsiteSource",
        "configured_profile": "discovery_crawl",
        "frozen_profile": "discovery_crawl",
    },
}

# These harness-owned keys exceed the shared redactor's conservative token
# length threshold. Unowned child-command keys must still pass through the
# shared redactor, even when they happen to be lowercase snake case.
REDACTION_SAFE_EVIDENCE_KEYS = frozenset({
    "deferred_post_multiphase",
    "production_bindings_exact",
    "workflow_status_relation",
})
DSN_URL_RE = re.compile(
    r"(?i)\b(?:postgres(?:ql)?|cockroachdb)"
    r"(?:\+[a-z0-9_.-]+)?://[^\s`'\"]+"
)


def _observed_source_execution(
    report: Mapping[str, Any],
) -> tuple[list[str], list[str]]:
    """Read exact source selection/completion from the ingest receipt."""
    completeness = report.get("completeness")
    if not isinstance(completeness, Mapping):
        raise RuntimeError("ingest report omitted its completeness receipt")
    selected = completeness.get("selected_sources")
    completed = completeness.get("completed_sources")
    if selected != ["docsite"] or completed != ["docsite"]:
        raise RuntimeError(
            "strict proof did not select and complete exactly docsite: "
            f"selected={selected!r} completed={completed!r}"
        )
    return list(selected), list(completed)


class PreexistingDatabaseError(RuntimeError):
    """The generated database name is owned by somebody else."""


class AmbiguousDatabaseCreateError(RuntimeError):
    """CREATE may have succeeded and therefore requires exact-name cleanup."""


class StrictIngestFailedError(RuntimeError):
    """Strict ingest failed with a structured, redaction-ready diagnostic."""

    def __init__(self, diagnostic: Mapping[str, Any]) -> None:
        super().__init__("strict docsite ingest failed")
        self.diagnostic = dict(diagnostic)


def _git(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )


def _head() -> str:
    result = _git("rev-parse", "HEAD")
    if result.returncode != 0:
        raise RuntimeError("cannot resolve Git HEAD")
    return result.stdout.strip()


def _tree_changes() -> list[str]:
    result = _git("status", "--porcelain=v1", "--untracked-files=all")
    if result.returncode != 0:
        raise RuntimeError("cannot inspect source tree")
    return [line for line in result.stdout.splitlines() if line]


def _repo_relative(path: Path) -> str:
    try:
        return path.resolve().relative_to(ROOT).as_posix()
    except ValueError as exc:
        raise RuntimeError("report output must be inside the repository") from exc


def _validate_database_name(database: str) -> None:
    if not re.fullmatch(rf"{DATABASE_PREFIX}[0-9a-f]{{12}}", database):
        raise RuntimeError(f"refusing unexpected database name {database!r}")


def _maintenance_and_target_dsns(base_dsn: str, database: str) -> tuple[str, str]:
    info = conninfo_to_dict(base_dsn)
    base_database = str(info.get("dbname") or "")
    if "test" not in base_database and "isolated" not in base_database:
        raise RuntimeError("OKG_TEST_DSN must name a test or isolated database")
    try:
        url = make_url(base_dsn)
    except Exception as exc:  # noqa: BLE001 - fail closed before any DB action
        raise RuntimeError("OKG_TEST_DSN must use a PostgreSQL URL") from exc
    maintenance = url.set(database="postgres").render_as_string(
        hide_password=False
    )
    target = url.set(database=database).render_as_string(hide_password=False)
    return maintenance, target


def _create_database(maintenance_dsn: str, database: str) -> None:
    _validate_database_name(database)
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
            except Exception as exc:  # noqa: BLE001 - side effect is ambiguous
                raise AmbiguousDatabaseCreateError(
                    f"CREATE DATABASE outcome is ambiguous: {type(exc).__name__}"
                ) from exc


def _drop_database(maintenance_dsn: str, database: str) -> dict[str, Any]:
    _validate_database_name(database)
    result: dict[str, Any] = {
        "attempted": True,
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
                    "SELECT EXISTS "
                    "(SELECT 1 FROM pg_database WHERE datname = %s)",
                    (database,),
                )
                result["absent"] = not bool(cur.fetchone()[0])
    except Exception as exc:  # noqa: BLE001 - teardown becomes proof data
        result["error"] = f"{type(exc).__name__}: {exc}"
    return result


def _run_owned_database(
    *,
    create: Callable[[], None],
    work: Callable[[], Any],
    drop: Callable[[], dict[str, Any]],
    teardown_sink: dict[str, Any],
) -> Any:
    """Drop only a database this invocation created or may have created."""
    cleanup_required = False
    try:
        try:
            create()
        except PreexistingDatabaseError:
            cleanup_required = False
            raise
        except AmbiguousDatabaseCreateError:
            cleanup_required = True
            raise
        cleanup_required = True
        return work()
    finally:
        if cleanup_required:
            teardown_sink.update(drop())


def _install_extensions(target_dsn: str) -> dict[str, Any]:
    from okg.substrate.db.migrate import ensure_required_extensions

    return ensure_required_extensions(target_dsn)


def _migrate(target_dsn: str) -> dict[str, Any]:
    from okg.substrate.db.migrate import migrate

    return asdict(migrate(target_dsn))


def _initialize_dbos(target_dsn: str, deployment_dir: Path) -> dict[str, Any]:
    from okg.substrate.deployments import resolve_deployment
    from okg.substrate.runtime.engine import with_dbos_singleton

    manifest = resolve_deployment(str(deployment_dir / "deployment.yaml"))
    with with_dbos_singleton(
        deployment=f"libcfg-{uuid.uuid4().hex[:12]}",
        dsn=target_dsn,
        runtime=manifest.runtime,
    ):
        pass
    with psycopg.connect(target_dsn, autocommit=True) as conn:
        relation = conn.execute(
            "SELECT to_regclass('dbos.workflow_status')"
        ).fetchone()[0]
    if relation is None:
        raise RuntimeError("DBOS schema was not initialized")
    return {"workflow_status_relation": str(relation)}


def _proof_nomos() -> dict[str, Any]:
    """Return an explicit, non-deferred privacy posture for the fixture."""
    return {
        "runtime_enforcement": "enforce",
        "deployment_posture": {
            "deployment_class": "ci_authority",
            "read_principals": ["ci_runner", "human_operator", "okg_service"],
            "write_principals": ["ci_runner", "okg_service"],
            "mcp_write_default": "deny",
            "export_policy": "internal_only",
            "secret_handling": "secret_ref_required",
            "live_call_default": "review",
            "allowed_source_sensitivities": [
                "internal",
                "credentialed_external",
            ],
            "allowed_data_classifications": ["internal"],
            "allowed_exportability": ["internal"],
            "privacy_obligations": ["audit", "secret_ref_required"],
            "readiness_profiles": ["ci"],
        },
        "source_policy_defaults": {
            "by_source_class": {
                "discovery_crawl": {
                    "store_raw": True,
                    "sensitivity": "internal",
                    "data_classification": "internal",
                    "credential_ref_policy": "none",
                    "live_call_allowed": False,
                    "exportability": "internal",
                    "retention": "normal",
                    "provenance": "local_filesystem",
                    "privacy_obligations": ["audit"],
                },
                "mutable_api": {
                    "store_raw": True,
                    "sensitivity": "credentialed_external",
                    "data_classification": "internal",
                    "credential_ref_policy": "optional",
                    "live_call_allowed": True,
                    "exportability": "internal",
                    "retention": "normal",
                    "provenance": "remote_api",
                    "privacy_obligations": ["audit", "review"],
                },
            }
        },
    }


def _materialize_deployment(root: Path, target_dsn: str) -> tuple[Path, str]:
    deployment = root / f"library-config-core-{uuid.uuid4().hex[:12]}"
    shutil.copytree(TEMPLATE, deployment)
    registry_path = deployment / "source_registry.yaml"
    document = fastyaml.safe_load(registry_path.read_text(encoding="utf-8"))
    if not isinstance(document, dict) or not isinstance(
        document.get("sources"), dict
    ):
        raise RuntimeError("shipped docs-corpus registry is malformed")
    document["admission_contract"] = "strict_v1"
    for source_name in ("docsite", "confluence"):
        source = document["sources"].get(source_name)
        if not isinstance(source, dict):
            raise RuntimeError(f"shipped registry lacks {source_name}")
        params = source.get("params")
        fixture = (
            params.get("fixture_path") if isinstance(params, dict) else None
        )
        anchor = "${deployment_dir}/"
        if not isinstance(fixture, str) or not fixture.startswith(anchor):
            raise RuntimeError(f"{source_name} fixture_path changed")
        source["params"] = {
            "fixture_path": str(
                (deployment / fixture.removeprefix(anchor)).resolve()
            )
        }
    registry_path.write_text(
        yaml.safe_dump(document, sort_keys=False),
        encoding="utf-8",
    )
    manifest_path = deployment / "deployment.yaml"
    manifest = fastyaml.safe_load(manifest_path.read_text(encoding="utf-8")) or {}
    name = f"library-config-core-{uuid.uuid4().hex[:12]}"
    manifest["name"] = name
    manifest["schema_dir"] = "schemas"
    manifest["source_registry"] = "source_registry.yaml"
    manifest["postgres"] = {"dsn": target_dsn}
    manifest["sync_root"] = str(ROOT)
    manifest["runtime"] = {"enabled": False}
    manifest["nomos"] = _proof_nomos()
    # The starter ships schemas/ (with a .gitkeep) since issue #2816.
    (deployment / "schemas").mkdir(exist_ok=True)
    manifest_path.write_text(
        yaml.safe_dump(manifest, sort_keys=False),
        encoding="utf-8",
    )
    return deployment, name


def _run_json(
    argv: list[str],
    *,
    target_dsn: str,
    secrets: tuple[str, ...],
) -> dict[str, Any]:
    env = {**os.environ, "OKG_DSN": target_dsn, "PYTHONPATH": str(ROOT / "src")}
    result = subprocess.run(
        argv,
        cwd=ROOT,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )
    try:
        payload = json.loads(result.stdout)
    except (TypeError, ValueError):
        payload = None
    return {
        "returncode": result.returncode,
        "payload": _redact(payload, secrets=secrets),
        "stderr": _redact(result.stderr[-4000:], secrets=secrets),
    }


def _redact(
    value: Any,
    *,
    secrets: tuple[str, ...],
    _field_name: str | None = None,
) -> Any:
    from okg.substrate.sources.redaction import (
        _key_names_secret,
        redact_text,
    )

    exact = tuple(
        item for item in (*secrets, str(ROOT)) if isinstance(item, str) and item
    )

    def clean_text(text: str) -> str:
        for secret in exact:
            text = text.replace(secret, "<redacted>")
        text = DSN_URL_RE.sub("<redacted-dsn>", text)
        return redact_text(text)

    if isinstance(value, Mapping):
        redacted: dict[str, Any] = {}
        for key, item in value.items():
            key_text = str(key)
            contains_exact_secret = any(
                secret in key_text for secret in exact
            )
            safe_key = (
                key_text
                if key_text in REDACTION_SAFE_EVIDENCE_KEYS
                and not contains_exact_secret
                else clean_text(key_text)
            )
            redacted[safe_key] = (
                "<redacted>"
                if _key_names_secret(key_text)
                else _redact(
                    item,
                    secrets=exact,
                    _field_name=key_text,
                )
            )
        return redacted
    if isinstance(value, list):
        return [
            _redact(item, secrets=exact, _field_name=_field_name)
            for item in value
        ]
    if isinstance(value, tuple):
        return tuple(
            _redact(item, secrets=exact, _field_name=_field_name)
            for item in value
        )
    if isinstance(value, str):
        if (
            _field_name in {"head_before", "head_after"}
            and re.fullmatch(r"[0-9a-f]{40,64}", value)
        ):
            return value
        return clean_text(value)
    return value


def _proof_secrets(*dsns: str) -> tuple[str, ...]:
    """Return exact DSN/password spellings that diagnostics must not persist."""
    values: set[str] = {dsn for dsn in dsns if dsn}
    for dsn in dsns:
        try:
            password = make_url(dsn).password
        except Exception:  # noqa: BLE001 - malformed DSN fails elsewhere
            password = None
        if password:
            values.update({
                password,
                quote(password, safe=""),
                quote_plus(password),
            })
    return tuple(sorted(values, key=len, reverse=True))


def _apply_catalog(
    *,
    target_dsn: str,
    deployment_dir: Path,
    secrets: tuple[str, ...],
) -> dict[str, Any]:
    base = [sys.executable, "-m", "okg.substrate.cli.main", "--json"]
    manifest = str(deployment_dir / "deployment.yaml")
    claim = _run_json(
        [
            *base,
            "catalog",
            "ownership",
            "claim",
            "--deployment",
            manifest,
            "--dsn",
            target_dsn,
        ],
        target_dsn=target_dsn,
        secrets=secrets,
    )
    load = _run_json(
        [
            *base,
            "catalog",
            "load",
            "--deployment",
            manifest,
            "--apply",
            "--dsn",
            target_dsn,
        ],
        target_dsn=target_dsn,
        secrets=secrets,
    )
    if claim["returncode"] != 0 or load["returncode"] != 0:
        raise RuntimeError("catalog ownership or load failed")
    return {"claim": claim, "load": load}


def _failed_ingest_diagnostic(report: Mapping[str, Any]) -> dict[str, Any]:
    """Keep only fail-path receipt fields for persisted diagnostics."""
    return {
        "results": [
            row
            for row in report.get("results", [])
            if isinstance(row, dict) and row.get("status") != "OK"
        ],
        "skipped": report.get("skipped", []),
        "completeness": report.get("completeness", {}),
        "publishes": report.get("publishes", []),
    }


def _publish(
    *,
    target_dsn: str,
    deployment_dir: Path,
    deployment: str,
) -> dict[str, Any]:
    from okg.substrate.ingest.adapter_factory import plan_source_registry_bindings
    from okg.substrate.ingest.service import IngestOptions, run_ingest
    from okg.substrate.sources.registry import load_source_registry_admission

    admission = load_source_registry_admission(
        deployment_dir / "source_registry.yaml",
        force_reload=True,
    )
    plans = plan_source_registry_bindings(admission, dsn=target_dsn).plans
    planned = sorted(plans)
    if planned != ["confluence", "docsite"]:
        raise RuntimeError(f"whole-document plans changed: {planned}")
    planned_bindings = {
        name: {
            "module": str(plan.entry.raw.get("module") or ""),
            "class": str(plan.entry.raw.get("class") or ""),
            "configured_profile": plan.entry.source_class,
            "frozen_profile": plan.executable_profile,
        }
        for name, plan in sorted(plans.items())
    }
    if planned_bindings != EXPECTED_PRODUCTION_BINDINGS:
        raise RuntimeError(
            f"production adapter bindings changed: {planned_bindings!r}"
        )
    outcome = run_ingest(
        IngestOptions(
            dsn=target_dsn,
            deployment=deployment,
            deployment_dir=deployment_dir,
            include={"docsite"},
            include_predecessors=False,
            run_mode="scope_complete",
            publish=True,
            command="strict library configuration isolated proof",
        )
    )
    if outcome.any_failed:
        raise StrictIngestFailedError(
            _failed_ingest_diagnostic(outcome.report)
        )
    selected, completed = _observed_source_execution(outcome.report)
    publishes = [
        row
        for row in outcome.report.get("publishes", [])
        if isinstance(row, dict) and row.get("status") == "published"
    ]
    if len(publishes) != 1 or not publishes[0].get("generation_id"):
        raise RuntimeError("strict docsite ingest did not publish once")
    return {
        "planned_sources": planned,
        "planned_bindings": planned_bindings,
        "selected_sources": selected,
        "completed_sources": completed,
        "selected_binding": planned_bindings[selected[0]],
        "publish": publishes[0],
    }


def _graph_counts(*, target_dsn: str, deployment: str) -> dict[str, int]:
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(target_dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        checkpoint = store.latest_generation(
            deployment_name=deployment,
            branch="default",
            status="published",
        )
        if checkpoint is None:
            store.close()
            return {"nodes": 0, "edges": 0}
        checkout = store.checkout_generation(checkpoint.checkpoint_id)
        try:
            nodes = checkout.query("SELECT node_id FROM okg.graph_nodes")
            edges = checkout.query("SELECT edge_id FROM okg.graph_edges")
        finally:
            close = getattr(checkout, "close", None)
            if callable(close):
                close()
            store.close()
    return {"nodes": len(nodes), "edges": len(edges)}


def _graph_and_queue(
    *,
    target_dsn: str,
    deployment: str,
    generation_id: str,
    before: Mapping[str, int],
) -> dict[str, Any]:
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.publish_proof import dlq_count

    with psycopg.connect(target_dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        checkpoint = store.latest_generation(
            deployment_name=deployment,
            branch="default",
            status="published",
        )
        if checkpoint is None or str(checkpoint.checkpoint_id) != generation_id:
            raise RuntimeError("published generation is not the latest checkpoint")
        checkout = store.checkout_generation(checkpoint.checkpoint_id)
        try:
            nodes = checkout.query("SELECT node_id FROM okg.graph_nodes")
            edges = checkout.query("SELECT edge_id FROM okg.graph_edges")
        finally:
            close = getattr(checkout, "close", None)
            if callable(close):
                close()
        store.close()
    dlq, queues = dlq_count(target_dsn)
    after = {"nodes": len(nodes), "edges": len(edges)}
    return {
        "before": dict(before),
        "after": after,
        "delta": {
            "nodes": after["nodes"] - int(before.get("nodes", 0)),
            "edges": after["edges"] - int(before.get("edges", 0)),
        },
        "active": sum(int(value) for value in queues.values()),
        "dlq": int(dlq),
    }


def _conditions(data: dict[str, Any]) -> dict[str, bool]:
    publish = data.get("publish") if isinstance(data.get("publish"), dict) else {}
    graph = data.get("graph") if isinstance(data.get("graph"), dict) else {}
    doctor = data.get("doctor") if isinstance(data.get("doctor"), dict) else {}
    doctor_payload = (
        doctor.get("payload") if isinstance(doctor.get("payload"), dict) else {}
    )
    teardown = (
        data.get("teardown") if isinstance(data.get("teardown"), dict) else {}
    )
    catalog = (
        data.get("catalog") if isinstance(data.get("catalog"), dict) else {}
    )
    catalog_load = (
        catalog.get("load") if isinstance(catalog.get("load"), dict) else {}
    )
    catalog_payload = (
        catalog_load.get("payload")
        if isinstance(catalog_load.get("payload"), dict)
        else {}
    )
    return {
        "extensions_ready": bool(data.get("extensions")),
        "schema_migrated": bool(data.get("migration")),
        "dbos_ready": bool(data.get("dbos")),
        "catalog_applied": catalog_load.get("returncode") == 0
        and catalog_payload.get("applied") is True,
        "whole_document_planned": data.get("planned_sources")
        == ["confluence", "docsite"],
        "production_bindings_exact": data.get("planned_bindings")
        == EXPECTED_PRODUCTION_BINDINGS,
        "docsite_only_selected": data.get("selected_sources") == ["docsite"],
        "docsite_only_completed": data.get("completed_sources")
        == ["docsite"],
        "selected_binding_exact": data.get("selected_binding")
        == EXPECTED_PRODUCTION_BINDINGS["docsite"],
        "generation_published": publish.get("status") == "published",
        "generation_present": isinstance(publish.get("generation_id"), str)
        and bool(publish.get("generation_id")),
        "delta_exact": graph.get("delta") == {"nodes": 2, "edges": 1},
        "queue_empty": graph.get("active") == 0 and graph.get("dlq") == 0,
        "doctor_clean": doctor.get("returncode") == 0
        and doctor_payload.get("ok") is True,
        "teardown_succeeded": teardown.get("drop_succeeded") is True,
        "database_absent": teardown.get("absent") is True,
        "head_unchanged": data.get("head_before") == data.get("head_after"),
        "tree_unchanged": data.get("tree_after") == [],
    }


def _envelope(
    *,
    status: str,
    generation_ref: str | None,
    artifact: str,
    data: dict[str, Any],
) -> dict[str, Any]:
    return {
        "evidence": [
            {
                "kind": "graph_report",
                "status": status,
                "refs": REPORT_REFS,
                "generation_ref": generation_ref,
                "command": (
                    "OKG_TEST_DSN=<redacted> python "
                    "scripts/prove_library_config_core_integration.py"
                ),
                "summary": (
                    "A marked copy of the shipped docs-corpus registry planned "
                    "both production adapters and published only DocsiteSource "
                    "with the expected isolated graph."
                    if status == "pass"
                    else "The isolated strict library configuration proof failed."
                ),
                "artifact": artifact,
                "data": data,
            }
        ]
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    # Required: the historical default was an evidence path inside one of
    # this repository's PACT change records, which the public copy lacks.
    parser.add_argument(
        "--out",
        type=Path,
        required=True,
        help="report path; it must not exist yet",
    )
    args = parser.parse_args(argv)
    out = args.out.resolve()
    artifact = _repo_relative(out)
    if out.exists():
        raise RuntimeError("designated report must be absent before proof")
    changes = _tree_changes()
    if changes:
        raise RuntimeError(f"refusing dirty source tree: {changes}")
    head_before = _head()
    base_dsn = os.environ.get("OKG_TEST_DSN")
    if not base_dsn:
        raise RuntimeError("OKG_TEST_DSN is required")
    database = f"{DATABASE_PREFIX}{uuid.uuid4().hex[:12]}"
    maintenance_dsn, target_dsn = _maintenance_and_target_dsns(
        base_dsn, database
    )
    secrets = _proof_secrets(base_dsn, maintenance_dsn, target_dsn)
    data: dict[str, Any] = {"head_before": head_before}
    teardown: dict[str, Any] = {}
    error: str | None = None

    def work() -> None:
        data["extensions"] = _install_extensions(target_dsn)
        data["migration"] = _migrate(target_dsn)
        with tempfile.TemporaryDirectory(prefix="okg-library-config-core-") as tmp:
            deployment_dir, deployment = _materialize_deployment(
                Path(tmp), target_dsn
            )
            data["dbos"] = _initialize_dbos(target_dsn, deployment_dir)
            data["catalog"] = _apply_catalog(
                target_dsn=target_dsn,
                deployment_dir=deployment_dir,
                secrets=secrets,
            )
            graph_before = _graph_counts(
                target_dsn=target_dsn,
                deployment=deployment,
            )
            published = _publish(
                target_dsn=target_dsn,
                deployment_dir=deployment_dir,
                deployment=deployment,
            )
            data.update(published)
            generation_id = str(published["publish"]["generation_id"])
            data["graph"] = _graph_and_queue(
                target_dsn=target_dsn,
                deployment=deployment,
                generation_id=generation_id,
                before=graph_before,
            )
            data["doctor"] = _run_json(
                [
                    sys.executable,
                    "-m",
                    "okg.substrate.cli.main",
                    "doctor",
                    "--dsn",
                    target_dsn,
                    "--json",
                ],
                target_dsn=target_dsn,
                secrets=secrets,
            )

    try:
        _run_owned_database(
            create=lambda: _create_database(maintenance_dsn, database),
            work=work,
            drop=lambda: _drop_database(maintenance_dsn, database),
            teardown_sink=teardown,
        )
    except Exception as exc:  # noqa: BLE001 - emit a safe failed report
        error = f"{type(exc).__name__}: {exc}"
        if isinstance(exc, StrictIngestFailedError):
            data["ingest_failure"] = exc.diagnostic
    data["teardown"] = teardown
    data["head_after"] = _head()
    data["tree_after"] = _tree_changes()
    conditions = _conditions(data)
    safe_data = _redact(
        {**data, "conditions": conditions, "error": error},
        secrets=secrets,
    )
    passed = error is None and all(conditions.values())
    generation_ref = None
    if isinstance(data.get("publish"), dict):
        candidate = data["publish"].get("generation_id")
        generation_ref = str(candidate) if candidate else None
    envelope = _envelope(
        status="pass" if passed else "fail",
        generation_ref=generation_ref,
        artifact=artifact,
        data=safe_data,
    )
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(envelope, indent=2, sort_keys=True) + "\n")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
