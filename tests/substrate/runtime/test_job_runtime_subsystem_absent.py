from __future__ import annotations

from pathlib import Path

from tests.mirror_scope import private_path


ROOT = Path(__file__).resolve().parents[3]


def test_job_runtime_python_package_and_workflow_are_absent() -> None:
    jobs_dir = ROOT / "src/okg/substrate/jobs"
    assert not list(jobs_dir.glob("*.py"))
    assert not (ROOT / "src/okg/substrate/runtime/workload_workflow.py").exists()


def test_canonical_schema_and_views_do_not_define_job_runtime_objects() -> None:
    schema = (ROOT / "src/okg/substrate/db/schema.sql").read_text(encoding="utf-8")
    views = (ROOT / "src/okg/substrate/db/views.sql").read_text(encoding="utf-8")
    for token in (
        "okg.job_definitions",
        "okg.job_dependencies",
        "okg.job_runs",
        "okg.job_chunks",
        "okg.job_items",
        "okg.job_item_frontiers",
        "okg.job_external_calls",
        "job_runtime_reconciliation",
        "okg.v_job_queue",
        "_workload:",
        "workload_source_fact_idx",
        "edge_facts_source_pattern_idx",
    ):
        assert token not in schema
        assert token not in views


def test_runtime_code_has_no_job_executor_or_compatibility_route() -> None:
    scanned = [
        ROOT / "src/okg/substrate/publisher/publisher.py",
        ROOT / "src/okg/substrate/source_sync_producers.py",
        ROOT / "src/okg/substrate/runtime/source_sync_workflow.py",
        ROOT / "src/okg/substrate/runtime/worker.py",
        ROOT / "src/okg/substrate/watch/watcher.py",
        ROOT / "src/okg/substrate/publish_proof.py",
        ROOT / "src/okg/substrate/exchange/hardening_harness.py",
        ROOT / "src/okg/substrate/library/templates/codebase-index/doctor_checks.py",
    ]
    forbidden = (
        "okg.substrate.jobs",
        "workload_delta_only",
        "_workload:",
        "route_via_dbos",
        "okg.job_definitions",
        "okg.job_runs",
        "okg.job_chunks",
        "okg.job_items",
        "okg.v_job_queue",
    )
    for path in scanned:
        source = path.read_text(encoding="utf-8")
        for token in forbidden:
            assert token not in source, f"{path.relative_to(ROOT)} contains {token}"

    management = (
        ROOT / "src/okg/substrate/db/management.py"
    ).read_text(encoding="utf-8")
    cli_main = (ROOT / "src/okg/substrate/cli/main.py").read_text(
        encoding="utf-8"
    )
    assert "def _legacy_job_summary" not in management
    assert '"legacy_jobs": _legacy_job_summary' not in management
    assert "legacy_queue_live_count" not in cli_main


def test_obsolete_cli_and_formal_surfaces_are_absent() -> None:
    main = (ROOT / "src/okg/substrate/cli/main.py").read_text(encoding="utf-8")
    parser_groups = (
        ROOT / "src/okg/substrate/cli/parser_groups.py"
    ).read_text(encoding="utf-8")
    for command in (
        '"reconcile-job-runtime"',
        '"retire-job-runtime-ledger"',
        '"retire-legacy-runtime"',
    ):
        assert command not in main
        assert command not in parser_groups
    assert '"lease"' not in parser_groups
    jobs_start = main.index("# jobs —")
    jobs_block = main[jobs_start:main.index("# profile", jobs_start)]
    for command in (
        '"dependencies"',
        '"external-calls"',
        '"pause"',
        '"resume"',
        '"cancel"',
        '"retry"',
    ):
        assert command not in jobs_block
    assert '"remove-job-runtime-subsystem"' in main

    migrations = ROOT / "src/okg/substrate/db/migrations"
    assert not (migrations / "retire_legacy_runtime.down.sql").exists()
    cleanup = (migrations / "0002_remove_job_runtime_subsystem.sql").read_text(
        encoding="utf-8"
    )
    assert " CASCADE" not in cleanup
    assert "DROP FUNCTION IF EXISTS okg.job_runtime_reconciliation()" in cleanup
    assert "cron.unschedule(legacy_job.jobid)" in cleanup
    for relation in (
        "okg.jobs",
        "okg.publisher_leases",
        "okg.job_definitions",
        "okg.job_runs",
        "okg.job_chunks",
        "okg.job_items",
    ):
        assert f"DROP TABLE IF EXISTS {relation}" in cleanup
    for index in (
        "okg.jobs_live_dedupe_uidx",
        "okg.node_facts_workload_source_fact_idx",
        "okg.edge_facts_workload_source_fact_idx",
        "okg.edge_facts_source_pattern_idx",
    ):
        assert f"DROP INDEX IF EXISTS {index}" in cleanup

    suite = (ROOT / "scripts/run_formal_suite.py").read_text(encoding="utf-8")
    for checker in (
        "check_formal_dbos_differential_job_runtime.py",
        "check_formal_generic_workload_runner.py",
        "check_formal_workload_delta_publish.py",
    ):
        assert checker not in suite
        assert not (ROOT / "scripts" / checker).exists()


def test_current_operator_docs_name_only_dbos_queue_authority() -> None:
    docs = (
        ROOT / "docs/develop/architecture/pipeline.md",
        ROOT / "docs/develop/architecture/runtime.md",
        ROOT / "docs/operate/deployments/runtime.md",
        ROOT / "docs/reference/mcp.md",
        ROOT / "docs/reference/database-schema.md",
        ROOT / "docs/runbooks/troubleshooting/storage.md",
        ROOT / "docs/operate/deployments/database.md",
        ROOT / "docs/runbooks/troubleshooting/runtime-queue.md",
    )
    forbidden = (
        "okg.v_job_queue",
        "okg.v_job_runtime_reconciliation",
        "okg.job_definitions",
        "src/okg/substrate/jobs",
    )
    for path in docs:
        text = path.read_text(encoding="utf-8")
        for token in forbidden:
            assert token not in text, f"{path.relative_to(ROOT)} contains {token}"


def test_workspace_doctor_names_no_job_queue_view() -> None:
    """The okg-workspace doctor arm, split out of the test above.

    deployments/okg-workspace is this repository's own deployment; the public
    copy skips this test (tests/mirror_scope.py).
    """
    deployed_doctor = private_path(
        "deployments/okg-workspace/doctor_checks_metrics.py"
    ).read_text(encoding="utf-8")
    assert "okg.v_job_queue" not in deployed_doctor
