from __future__ import annotations

import ast
import re
from pathlib import Path
from typing import Any

import yaml

from tests.mirror_scope import private_path


ROOT = Path(__file__).resolve().parents[3]
MANIFEST = ROOT / "src/okg/substrate/runtime/authority_manifest.yaml"

REQUIRED_SURFACE_GROUPS = {
    "admission",
    "dogfood",
    "jobs",
    "publisher",
    "runtime",
    "source-sync",
}
REQUIRED_AUTHORITY_CLASSES = {
    "dbos_executor",
    "explicit_inline_operator_path",
    "read_only_reconciliation_surface",
    "source_progress_writer",
}
PRODUCTION_ENTRY_IDS = {
    "source_sync.producer.enqueue_events",
    "source_sync.dbos_enqueue_helpers",
    "source_sync.workflow_body",
    "publisher.cli.dbos_run_once",
    "publisher.workflow_body",
    "ingest.cli.source_sync_dbos",
    "jobs.cli.enqueue_source",
    "admission.cli.submit_decide_dbos",
    "runtime.admin.dbos_client_enqueues",
}
PROGRESS_WRITER_ENTRY_IDS = {
    "source_sync.producer.progress_writer",
    "source_sync.ingest.progress_writer",
    "source_sync.terminal_state.progress_writer",
    "source_sync.watcher.progress_writer",
    "source_sync.admission_rollback.progress_writer",
    "publisher.projection_watermark_writer",
    "source_sync.microbench.progress_writer",
    "runtime.telemetry_probe.progress_writer",
}


def _load_manifest() -> dict[str, Any]:
    payload = yaml.safe_load(MANIFEST.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def _source_for(entry: dict[str, Any]) -> str:
    path = ROOT / str(entry["path"])
    assert path.exists(), entry["id"]
    return path.read_text(encoding="utf-8")


def _python_defs(source: str) -> set[str]:
    tree = ast.parse(source)
    return {
        node.name
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
    }


def test_runtime_authority_manifest_covers_required_surfaces() -> None:
    payload = _load_manifest()

    assert payload["schema"] == "okg.runtime_authority_inventory.v1"
    assert set(payload["allowed_authority_classes"]) == REQUIRED_AUTHORITY_CLASSES

    entries = payload["entry_points"]
    assert isinstance(entries, list)
    assert {entry["surface_group"] for entry in entries} >= REQUIRED_SURFACE_GROUPS
    assert {entry["authority_class"] for entry in entries} == REQUIRED_AUTHORITY_CLASSES
    assert {entry["id"] for entry in entries} >= PRODUCTION_ENTRY_IDS


def test_runtime_authority_manifest_points_at_real_symbols_and_markers() -> None:
    payload = _load_manifest()

    for entry in payload["entry_points"]:
        source = _source_for(entry)
        defs = _python_defs(source)
        for symbol in entry.get("symbols") or ():
            assert symbol in defs, entry["id"]
        for marker in entry.get("markers") or ():
            assert marker in source, entry["id"]


def test_production_execution_entries_are_dbos_only() -> None:
    payload = _load_manifest()

    for entry in payload["entry_points"]:
        if not entry.get("production_execution"):
            continue
        assert entry["authority_class"] == "dbos_executor", entry["id"]
        assert entry["dbos_lifecycle_authority"] is True, entry["id"]
        assert entry["dbos_required_when_runtime_enabled"] is True, entry["id"]
        assert entry["legacy_okg_job_executor_allowed"] is False, entry["id"]
        assert entry["progress_ledger_only"] is False, entry["id"]


def test_explicit_operator_paths_are_not_silent_legacy_fallbacks() -> None:
    payload = _load_manifest()
    operator_entries = [
        entry for entry in payload["entry_points"]
        if entry["authority_class"] == "explicit_inline_operator_path"
    ]

    assert operator_entries
    for entry in operator_entries:
        assert entry["production_execution"] is False, entry["id"]
        assert entry["dbos_lifecycle_authority"] is False, entry["id"]
        assert entry["legacy_okg_job_executor_allowed"] is False, entry["id"]
        assert entry["progress_ledger_only"] is False, entry["id"]


def test_source_progress_writers_are_bookkeeping_only() -> None:
    payload = _load_manifest()
    entries = {
        entry["id"]: entry for entry in payload["entry_points"]
    }

    assert entries.keys() >= PROGRESS_WRITER_ENTRY_IDS
    for entry_id in PROGRESS_WRITER_ENTRY_IDS:
        entry = entries[entry_id]
        assert entry["authority_class"] == "source_progress_writer"
        assert entry["production_execution"] is False
        assert entry["dbos_lifecycle_authority"] is False
        assert entry["legacy_okg_job_executor_allowed"] is False
        assert entry["progress_ledger_only"] is True


def test_manifest_covers_every_direct_progress_and_watermark_writer() -> None:
    payload = _load_manifest()
    writer_paths = {
        str(entry["path"])
        for entry in payload["entry_points"]
        if entry["authority_class"] == "source_progress_writer"
    }
    writer_dml = re.compile(
        r"(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+"
        r"okg\.(?:source_progress|source_watermarks)",
        re.IGNORECASE,
    )
    substrate = ROOT / "src/okg/substrate"
    discovered = {
        str(path.relative_to(ROOT))
        for path in substrate.rglob("*.py")
        if writer_dml.search(path.read_text(encoding="utf-8"))
    }

    assert discovered == writer_paths


def test_manifest_contains_no_retired_job_runtime_surfaces() -> None:
    payload = _load_manifest()
    entries = payload["entry_points"]
    assert all(entry["authority_class"] != "retired_legacy_surface" for entry in entries)
    assert all("reconcile_job_runtime" not in entry["id"] for entry in entries)
    producer = next(
        entry for entry in entries
        if entry["id"] == "source_sync.producer.enqueue_events"
    )
    assert producer["legacy_okg_job_executor_allowed"] is False
    assert producer["dbos_required_when_runtime_enabled"] is True


def test_production_python_tree_has_no_job_runtime_execution_path() -> None:
    substrate = ROOT / "src/okg/substrate"
    job_runtime_dml = re.compile(
        r"(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+"
        r"okg\.(?:jobs|publisher_leases|job_[a-z_]+)",
        re.IGNORECASE,
    )
    forbidden_tokens = (
        "okg.substrate.jobs",
        "route_via_dbos",
        "workload_delta_only",
        "_workload:",
    )

    for path in substrate.rglob("*.py"):
        source = path.read_text(encoding="utf-8")
        for token in forbidden_tokens:
            assert token not in source, f"{path.relative_to(ROOT)} contains {token}"
        assert job_runtime_dml.search(source) is None, (
            f"{path.relative_to(ROOT)} writes obsolete job-runtime state"
        )


def test_workspace_doctor_reads_no_job_queue_view() -> None:
    """The okg-workspace doctor arm, split out of the production-tree scan above.

    deployments/okg-workspace is this repository's own deployment; the public
    copy skips this test (tests/mirror_scope.py).
    """
    deployed_doctor = private_path(
        "deployments/okg-workspace/doctor_checks_metrics.py"
    ).read_text(encoding="utf-8")
    assert "okg.v_job_queue" not in deployed_doctor
