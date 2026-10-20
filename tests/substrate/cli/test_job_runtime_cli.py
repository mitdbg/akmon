"""CLI tests for the DBOS-only runtime operator surface."""
from __future__ import annotations

import argparse
import importlib
import json
import os
import subprocess
from types import SimpleNamespace

import pytest

from okg.substrate.cli import jobs as jobs_cli
from okg.substrate.cli.jobs import _cmd_jobs
pytestmark = pytest.mark.postgres


def _args(**kwargs):
    defaults = {
        "deployment": None,
        "dsn": None,
        "apply": True,
    }
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


def test_jobs_parser_exposes_only_dbos_runtime_surfaces() -> None:
    from okg.substrate.cli.main import _build_parser

    parser = _build_parser()
    jobs_parser = None
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            jobs_parser = action.choices["jobs"]
            break
    assert jobs_parser is not None
    jobs_subcommands = set()
    for action in jobs_parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            jobs_subcommands = set(action.choices)
            break

    assert jobs_subcommands == {"status", "enqueue-source", "find-source-run"}
    read_args = parser.parse_args(["jobs", "status"])
    assert not hasattr(read_args, "apply")
    write_args = parser.parse_args(
        ["jobs", "enqueue-source", "--source", "docs", "--apply"]
    )
    assert write_args.apply is True


def test_run_canonicalizes_path_deployment_identity(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    from okg.substrate.cli import main as cli
    import okg.substrate.catalog.modules_compose as compose_mod
    import okg.substrate.catalog.ownership as ownership_mod
    import okg.substrate.profiling as profiling_mod
    import okg.substrate.publisher.publisher as publisher_pkg
    import okg.substrate.publisher.deployment_stack as stack_mod
    import okg.substrate.runtime.admin as runtime_admin

    deployment_dir = tmp_path / "deployment-by-path"
    deployment_dir.mkdir()
    (deployment_dir / "schemas").mkdir()
    (deployment_dir / "source_registry.yaml").write_text(
        "sources: []\n", encoding="utf-8")
    (deployment_dir / "deployment.yaml").write_text(
        "\n".join([
            "name: canonical-run-deployment",
            "description: test",
            "schema_dir: schemas",
            "source_registry: source_registry.yaml",
            "postgres:",
            "  dsn: postgres://example/okg",
            "runtime:",
            "  enabled: false",
            "",
        ]),
        encoding="utf-8",
    )
    git_env = {
        **os.environ,
        "GIT_AUTHOR_NAME": "OKG fixture",
        "GIT_AUTHOR_EMAIL": "fixture@example.invalid",
        "GIT_COMMITTER_NAME": "OKG fixture",
        "GIT_COMMITTER_EMAIL": "fixture@example.invalid",
    }
    subprocess.run(
        ["git", "init", "-q", "-b", "main"],
        cwd=deployment_dir,
        env=git_env,
        check=True,
    )
    subprocess.run(
        ["git", "add", "-A"], cwd=deployment_dir, env=git_env, check=True,
    )
    subprocess.run(
        ["git", "commit", "-q", "-m", "fixture"],
        cwd=deployment_dir,
        env=git_env,
        check=True,
    )

    seen: dict[str, object] = {}

    class FakeStack:
        deployment_name = "canonical-run-deployment"
        enrichers = ()

        def publisher_kwargs(self):
            return {}

    class FakePublisher:
        def __init__(self, dsn, deployment_name, **kwargs):
            seen["publisher"] = {
                "dsn": dsn,
                "deployment_name": deployment_name,
                "kwargs": kwargs,
            }

        def set_owner(self, **kwargs):
            seen["owner"] = kwargs

        def cycle(self, *, force_enrichers=False):
            assert force_enrichers is False
            return {
                "status": "published",
                "generation_id": "okg:canonical-run-deployment:gen:test",
                "compat_generation_id": 1,
            }

    class FakeProfile:
        generation_id = None

        def __enter__(self):
            seen["profile"] = self
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(compose_mod, "compose_catalog", lambda dep_dir: object())
    monkeypatch.setattr(
        ownership_mod,
        "catalog_identity_for",
        lambda **kwargs: SimpleNamespace(catalog_fingerprint="catalog:test"),
    )
    monkeypatch.setattr(publisher_pkg, "Publisher", FakePublisher)

    def fake_build_deployment_publisher_stack(**kwargs):
        seen["stack"] = kwargs
        return FakeStack()

    def fake_profiling_context(**kwargs):
        seen["profiling_kwargs"] = kwargs
        return FakeProfile()

    monkeypatch.setattr(
        stack_mod,
        "build_deployment_publisher_stack",
        fake_build_deployment_publisher_stack,
    )
    monkeypatch.setattr(
        profiling_mod,
        "profiling_context",
        fake_profiling_context,
    )
    monkeypatch.setattr(cli, "_publisher_kwargs_from_env", lambda: {})
    monkeypatch.setattr(cli, "_profiling_policy_for_args", lambda args: None)
    monkeypatch.setattr(cli, "_profile_force_deep", lambda args: False)
    # `okg run --once` answers "is this database ready" before any graph work
    # (issue #2128). This test asserts deployment-identity canonicalization
    # against a manifest whose DSN names the unreachable host `example`, so
    # the readiness precondition is stubbed to its success answer like the
    # publisher stack above -- a ready database with nothing pending,
    # since the caller now reports what the probe hands back.
    from okg.substrate.db.readiness import DatabaseReadiness

    monkeypatch.setattr(
        "okg.substrate.db.readiness.require_database_ready",
        lambda dsn, **kwargs: DatabaseReadiness(
            schema_present=True, schema_applied=True, ledger_present=True
        ),
    )
    monkeypatch.setattr(
        cli,
        "_run_inline_publish_cycle",
        lambda **kwargs: {
            "status": "published",
            "generation_id": "okg:canonical-deployment:branch:default:gen:1",
            "admission_feedback": {
                "status": "published",
                "generation_id": (
                    "okg:canonical-deployment:branch:default:gen:1"
                ),
                "branch_name": "default",
                "updated": 0,
                "admission_ids": [],
            },
        },
    )

    rc = cli._cmd_run(
        _args(
            once=True,
            deployment=str(deployment_dir),
            owner_id=None,
            owner_meta="{}",
            include_llm_invariants=False,
            llm_invariant=None,
            inline=True,
            branch=None,
            profile=None,
            profile_collector=(),
        ),
        as_json=True,
    )

    assert rc == 0
    assert seen["publisher"]["deployment_name"] == "canonical-run-deployment"
    assert seen["stack"]["deployment_name"] == "canonical-run-deployment"
    assert seen["profiling_kwargs"]["deployment"] == "canonical-run-deployment"
    assert '"status": "published"' in capsys.readouterr().out

    runtime_deployment_dir = tmp_path / "runtime-deployment-by-path"
    runtime_deployment_dir.mkdir()
    (runtime_deployment_dir / "schemas").mkdir()
    (runtime_deployment_dir / "source_registry.yaml").write_text(
        "sources: []\n", encoding="utf-8")
    (runtime_deployment_dir / "deployment.yaml").write_text(
        "\n".join([
            "name: canonical-runtime-deployment",
            "description: test",
            "schema_dir: schemas",
            "source_registry: source_registry.yaml",
            "postgres:",
            "  dsn: postgres://example/runtime",
            "runtime:",
            "  enabled: true",
            "",
        ]),
        encoding="utf-8",
    )
    enqueued: list[dict[str, object]] = []

    def fake_client_enqueue_publisher_cycle(**kwargs):
        enqueued.append(kwargs)
        return SimpleNamespace(
            workflow_id="wf-canonical-runtime",
            status="SUCCESS",
            elapsed_seconds=0.1,
            summary={
                "generation_id": (
                    "okg:canonical-runtime-deployment:gen:test"
                ),
                "status": "published",
            },
            error=None,
        )

    monkeypatch.setattr(
        runtime_admin,
        "client_enqueue_publisher_cycle",
        fake_client_enqueue_publisher_cycle,
    )

    rc = cli._cmd_run(
        _args(
            once=True,
            deployment=str(runtime_deployment_dir),
            owner_id=None,
            owner_meta="{}",
            include_llm_invariants=False,
            llm_invariant=None,
            inline=False,
            branch=None,
            force_enrichers=True,
            profile=None,
            profile_collector=(),
        ),
        as_json=True,
    )

    assert rc == 0
    assert enqueued == [{
        "deployment": "canonical-runtime-deployment",
        "dsn": "postgres://example/runtime",
        # #1311: --branch used to be dropped on the DBOS path.
        "branch_name": "default",
        "force_enrichers": True,
        # #2502: only the CLI bounds the wait for a worker to take the row.
        "dequeue_timeout_seconds": 30.0,
    }]


def test_jobs_enqueue_source_uses_dbos_workflow(
    clean_runtime: str,
    monkeypatch,
    capsys,
) -> None:
    enqueued: list[dict[str, object]] = []

    def fake_enqueue_source_sync_via_dbos(**kwargs):
        enqueued.append(kwargs)
        return "wf-cli-source"

    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue.enqueue_source_sync_via_dbos",
        fake_enqueue_source_sync_via_dbos,
    )

    rc = _cmd_jobs(
        _args(
            jobs_subcmd="enqueue-source",
            dsn=clean_runtime,
            runtime_deployment="test",
            source="docs",
            scope_key="README.md",
            run_mode="scope_complete",
            dependent_fanout="closure",
            run_key="cli-source-enqueue",
            no_publish=False,
        ),
        as_json=True,
    )
    enqueue_payload = json.loads(capsys.readouterr().out)

    assert rc == 0
    assert enqueue_payload["dbos_workflow_id"] == "wf-cli-source"
    assert enqueue_payload["deduplication_id"] == "cli-source-enqueue"
    assert enqueue_payload["execution_authority"] == "dbos.workflow_status"
    assert enqueued[0]["deployment"] == "test"
    assert enqueued[0]["deduplication_id"] == "cli-source-enqueue"
    assert enqueued[0]["job"].sources == ("docs",)


def test_jobs_status_reports_dbos_counts_without_reinterpreting_failures(
    monkeypatch,
) -> None:
    class _Cursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class _Connection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self):
            return _Cursor()

    monkeypatch.setattr(jobs_cli, "db_connect", lambda dsn: _Connection())
    monkeypatch.setattr(jobs_cli, "_relation_exists", lambda cur, relation: False)
    monkeypatch.setattr(jobs_cli, "_procedure_exists", lambda cur, procedure: False)
    monkeypatch.setattr(jobs_cli, "job_runtime_cron_schedules", lambda cur: [])
    monkeypatch.setattr(
        jobs_cli,
        "_dbos_workflow_status_report",
        lambda cur: {
            "dbos_workflow_status_installed": True,
            "dbos_workflow_counts": [
                {
                    "workflow_name": "okg_publisher_cycle",
                    "status": "SUCCESS",
                    "count": 1,
                },
                {
                    "workflow_name": "okg_source_sync",
                    "status": "ENQUEUED",
                    "count": 1,
                },
                {
                    "workflow_name": "okg_embed_batch",
                    "status": "MAX_RECOVERY_ATTEMPTS_EXCEEDED",
                    "count": 1,
                },
            ],
            "dbos_publisher_counts": [{"status": "SUCCESS", "count": 1}],
            "dbos_source_sync_counts": [{"status": "ENQUEUED", "count": 1}],
            "dbos_embed_counts": [
                {"status": "MAX_RECOVERY_ATTEMPTS_EXCEEDED", "count": 1}
            ],
        },
    )

    status_payload = jobs_cli._status_report(
        "postgres://example/test",
        deployment="demo",
    )

    assert status_payload["dbos_workflow_status_installed"] is True
    assert any(
        row["status"] == "SUCCESS" and row["count"] >= 1
        for row in status_payload["dbos_publisher_counts"]
    )
    assert any(
        row["status"] == "ENQUEUED" and row["count"] >= 1
        for row in status_payload["dbos_source_sync_counts"]
    )
    assert any(
        row["status"] == "MAX_RECOVERY_ATTEMPTS_EXCEEDED" and row["count"] >= 1
        for row in status_payload["dbos_embed_counts"]
    )
    assert "queue_health" not in status_payload
    assert status_payload["execution_authority"] == "dbos.workflow_status"
    assert status_payload["okg_job_runtime_subsystem"] == "absent"
    assert status_payload["obsolete_objects_present"] == []
    assert status_payload["obsolete_cron_schedules_present"] == []


def test_jobs_status_is_unavailable_without_dbos_schema(monkeypatch) -> None:
    class _Cursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class _Connection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self):
            return _Cursor()

    monkeypatch.setattr(jobs_cli, "db_connect", lambda dsn: _Connection())
    monkeypatch.setattr(
        jobs_cli,
        "_dbos_workflow_status_report",
        lambda cur: {
            "dbos_workflow_status_installed": False,
            "dbos_workflow_counts": [],
            "dbos_source_sync_counts": [],
            "dbos_publisher_counts": [],
            "dbos_embed_counts": [],
        },
    )
    monkeypatch.setattr(jobs_cli, "_relation_exists", lambda cur, relation: False)
    monkeypatch.setattr(jobs_cli, "_procedure_exists", lambda cur, procedure: False)
    monkeypatch.setattr(jobs_cli, "job_runtime_cron_schedules", lambda cur: [])

    payload = jobs_cli._status_report("postgres://example/test", deployment="demo")

    assert payload["status"] == "unavailable"
    assert payload["ok"] is False
    assert payload["okg_job_runtime_subsystem"] == "absent"
    assert payload["obsolete_objects_present"] == []
    assert payload["obsolete_cron_schedules_present"] == []
    assert payload["dbos_workflow_status_installed"] is False


@pytest.mark.parametrize(
    ("obsolete_relation", "obsolete_cron_schedules"),
    (
        ("okg.jobs", ()),
        ("okg.job_runs", ()),
        (None, ("okg-publish",)),
    ),
)
def test_jobs_status_requires_cleanup_when_obsolete_schema_remains(
    monkeypatch,
    obsolete_relation: str | None,
    obsolete_cron_schedules: tuple[str, ...],
) -> None:
    class _Cursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class _Connection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self):
            return _Cursor()

    monkeypatch.setattr(jobs_cli, "db_connect", lambda dsn: _Connection())
    monkeypatch.setattr(
        jobs_cli,
        "_dbos_workflow_status_report",
        lambda cur: {
            "dbos_workflow_status_installed": True,
            "dbos_workflow_counts": [],
            "dbos_source_sync_counts": [],
            "dbos_publisher_counts": [],
            "dbos_embed_counts": [],
        },
    )
    monkeypatch.setattr(
        jobs_cli,
        "_relation_exists",
        lambda cur, relation: relation == obsolete_relation,
    )
    monkeypatch.setattr(jobs_cli, "_procedure_exists", lambda cur, procedure: False)
    monkeypatch.setattr(
        jobs_cli,
        "job_runtime_cron_schedules",
        lambda cur: list(obsolete_cron_schedules),
    )

    payload = jobs_cli._status_report("postgres://example/test", deployment="demo")

    assert payload["status"] == "cleanup_required"
    assert payload["ok"] is False
    assert payload["obsolete_objects_present"] == (
        [obsolete_relation] if obsolete_relation is not None else []
    )
    assert payload["obsolete_cron_schedules_present"] == list(
        obsolete_cron_schedules
    )
    assert payload["okg_job_runtime_subsystem"] == "cleanup_required"


@pytest.mark.parametrize(
    "subcmd",
    (
        "inspect",
        "dependencies",
        "external-calls",
        "pause",
        "resume",
        "cancel",
        "retry",
        "microbench",
    ),
)
def test_obsolete_job_commands_are_not_parseable(subcmd: str) -> None:
    from okg.substrate.cli.main import _build_parser

    with pytest.raises(SystemExit) as exc_info:
        _build_parser().parse_args(["jobs", subcmd])
    assert exc_info.value.code != 0


def test_obsolete_job_runtime_package_is_not_importable() -> None:
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("okg.substrate.jobs")
