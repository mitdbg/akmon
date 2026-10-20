from __future__ import annotations

import hashlib
import importlib
import inspect
import json
import os
import shutil
import subprocess
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from types import FrameType, ModuleType
from typing import Any

import pytest

import okg.substrate.source_sync_microbench as ledger_facade
import okg.substrate.source_sync_optimization_ledger as ledger_owner
from okg.substrate.cli import deployment_readiness


REPO_ROOT = Path(__file__).resolve().parents[2]
#: Fixed-time ledger characterization recorded by PACT
#: extract-source-sync-optimization-ledger.
CHARACTERIZATION_PATH = (
    REPO_ROOT
    / "tests/fixtures/source_sync_optimization_ledger"
    / "current-dev-ledger-characterization.json"
)

EXPECTED_FACADE_ALL = (
    "MICROBENCH_INVENTORY_SCHEMA",
    "MICROBENCH_OPTIMIZATION_LEDGER_ROWS_SCHEMA",
    "MICROBENCH_OPTIMIZATION_LEDGER_SCHEMA",
    "MICROBENCH_RUN_SCHEMA",
    "MICROBENCH_VIEW_ROWS_SCHEMA",
    "build_source_optimization_ledger",
    "build_microbenchmark_inventory",
    "build_source_sync_microbenchmark_report",
    "microbenchmark_inventory_relation_rows",
    "source_optimization_ledger_relation_rows",
)

MOVED_SYMBOLS = (
    "MICROBENCH_OPTIMIZATION_LEDGER_SCHEMA",
    "MICROBENCH_OPTIMIZATION_LEDGER_ROWS_SCHEMA",
    "SUBSECOND_LATENCY_BUDGET_MS",
    "HOT_WAVE_SOURCE_ORDER",
    "build_source_optimization_ledger",
    "rank_source_optimization_rows",
    "source_optimization_ledger_relation_rows",
)


def _characterization() -> dict[str, Any]:
    return json.loads(CHARACTERIZATION_PATH.read_text(encoding="utf-8"))


def _canonical_sha256(payload: Any) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


@contextmanager
def _record_owner_builder_calls() -> Iterator[list[tuple[str, str]]]:
    owner_code = ledger_owner.build_source_optimization_ledger.__code__
    owner_source = str(Path(ledger_owner.__file__).resolve())
    calls: list[tuple[str, str]] = []
    previous_profile = sys.getprofile()

    def _profile(frame: FrameType, event: str, arg: Any) -> None:
        if previous_profile is not None:
            previous_profile(frame, event, arg)
        if (
            event == "call"
            and frame.f_code is owner_code
            and frame.f_globals.get("__name__")
            == "okg.substrate.source_sync_optimization_ledger"
            and str(Path(frame.f_code.co_filename).resolve()) == owner_source
        ):
            calls.append((str(frame.f_globals["__name__"]), owner_source))

    sys.setprofile(_profile)
    try:
        yield calls
    finally:
        sys.setprofile(previous_profile)


def _install_subprocess_owner_profiler(
    tmp_path: Path,
    env: dict[str, str],
) -> Path:
    profiler_dir = tmp_path / "owner-call-profiler"
    profiler_dir.mkdir()
    marker_path = tmp_path / "owner-builder-called.txt"
    owner_source = str(Path(ledger_owner.__file__).resolve())
    (profiler_dir / "sitecustomize.py").write_text(
        "import os as _os\n"
        "import sys as _sys\n"
        "from pathlib import Path as _Path\n"
        "_expected_source = _Path(\n"
        "    _os.environ['SLICE3_OWNER_SOURCE']\n"
        ").resolve()\n"
        "_marker = _Path(_os.environ['SLICE3_OWNER_PROFILE_MARKER'])\n"
        "def _slice3_profile(frame, event, arg):\n"
        "    if (\n"
        "        event == 'call'\n"
        "        and frame.f_code.co_name\n"
        "        == 'build_source_optimization_ledger'\n"
        "        and frame.f_globals.get('__name__')\n"
        "        == 'okg.substrate.source_sync_optimization_ledger'\n"
        "        and _Path(frame.f_code.co_filename).resolve()\n"
        "        == _expected_source\n"
        "    ):\n"
        "        _marker.write_text(\n"
        "            f\"{frame.f_globals['__name__']}:\"\n"
        '            f"{frame.f_code.co_name}:"\n'
        '            f"{_expected_source}\\n",\n'
        "            encoding='utf-8',\n"
        "        )\n"
        "        _sys.setprofile(None)\n"
        "_sys.setprofile(_slice3_profile)\n",
        encoding="utf-8",
    )
    existing_pythonpath = env.get("PYTHONPATH")
    pythonpath_parts = [str(profiler_dir)]
    if existing_pythonpath:
        pythonpath_parts.append(existing_pythonpath)
    env["PYTHONPATH"] = os.pathsep.join(pythonpath_parts)
    env["SLICE3_OWNER_SOURCE"] = owner_source
    env["SLICE3_OWNER_PROFILE_MARKER"] = str(marker_path)
    return marker_path


def _single_source_inventory(source: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema": "okg.source_sync.microbench.inventory.v1",
        "deployment": "slice3-test",
        "generated_at": "2026-08-16T11:59:00+00:00",
        "source_count": 1,
        "enricher_count": 0,
        "live_state": {"status": "loaded"},
        "coverage_counts": {"automatic_hot_path": 1},
        "sources": [source],
        "enrichers": [],
    }


def _measured_source(**overrides: Any) -> dict[str, Any]:
    source = {
        "source_name": "agent-sessions",
        "owner": "slice3.agent-sessions",
        "coverage_class": "automatic_hot_path",
        "readiness_required": True,
        "latest_source_runner_ms": 125.0,
        "latest_stage_total_ms": 200.0,
        "latest_nodes_appended": 1,
        "latest_edges_appended": 0,
        "latest_dedupe_drops_node": 0,
        "latest_dedupe_drops_edge": 0,
        "scope_support_status": "declared",
        "cursor_support_status": "incremental_verified",
        "producer_status": "configured",
        "benchmark_scenarios": ["source-only", "full-pipeline"],
        "blocker": None,
    }
    source.update(overrides)
    return source


def test_fixed_time_characterization_matches_current_dev_artifact() -> None:
    artifact = _characterization()
    expected = artifact["expected"]
    inventory = deepcopy(artifact["inventory"])
    inventory_before = deepcopy(inventory)

    ledger = ledger_owner.build_source_optimization_ledger(
        inventory,
        generated_at=artifact["fixed_generated_at"],
    )
    ledger_before_relation_rows = deepcopy(ledger)
    relation_rows = ledger_owner.source_optimization_ledger_relation_rows(ledger)

    assert inventory == inventory_before
    assert ledger == ledger_before_relation_rows
    assert ledger["generated_at"] == artifact["fixed_generated_at"]
    assert ledger["schema"] == expected["ledger_schema"]
    assert (
        ledger_owner.MICROBENCH_OPTIMIZATION_LEDGER_SCHEMA
        == (expected["ledger_schema"])
    )
    assert (
        ledger_owner.MICROBENCH_OPTIMIZATION_LEDGER_ROWS_SCHEMA
        == (expected["relation_rows_schema"])
    )
    assert ledger["row_count"] == expected["row_count"]
    assert ledger["source_count"] == expected["source_count_metadata"]
    assert ledger["enricher_count"] == expected["enricher_count_metadata"]
    assert [row["name"] for row in ledger["rows"]] == expected["ordered_names"]
    assert [row["priority_rank"] for row in ledger["rows"]] == (
        expected["priority_ranks"]
    )
    assert ledger["optimization_status_counts"] == expected["status_counts"]
    assert ledger["optimization_wave_counts"] == expected["wave_counts"]
    assert len(relation_rows) == expected["relation_row_count"]
    assert (
        _canonical_sha256(
            {
                "ledger": ledger,
                "relation_rows": relation_rows,
            }
        )
        == expected["canonical_payload_sha256"]
    )
    assert (
        _canonical_sha256(sorted(relation_rows[0])) == (expected["relation_key_sha256"])
    )

    rows_by_name = {row["name"]: row for row in ledger["rows"]}
    assert rows_by_name[""]["latest_source_runner_ms"] is None
    assert rows_by_name[""]["latest_nodes_appended"] == "bad"
    assert rows_by_name["doc-corpus"]["proof_status"] == (
        "missing_source_runner_sample"
    )
    assert rows_by_name["package-manifests"]["optimization_status"] == (
        "blocked_inventory"
    )
    assert rows_by_name["package-manifests"]["amplification_signal"] == (
        "dedupe_drops_after_source_work"
    )
    assert rows_by_name["agent-sessions"]["proof_status"] == (
        "full_pipeline_benchmark_passed"
    )
    assert (
        rows_by_name["agent-sessions"]["latest_full_pipeline_no_change_dominant"]
        is True
    )
    assert rows_by_name["code-data-flow"]["optimization_wave"] == "policy_wave"
    assert rows_by_name["ProfileHotspotLinker"]["optimization_wave"] == (
        "enricher_wave"
    )

    rank_input = list(reversed(deepcopy(relation_rows)))
    rank_input_before = deepcopy(rank_input)
    ranked = ledger_owner.rank_source_optimization_rows(rank_input)
    assert rank_input == rank_input_before
    assert [row["priority_rank"] for row in ranked] == list(
        range(1, len(ranked) + 1),
    )


@pytest.mark.parametrize(
    ("include_no_change", "no_change_value"),
    ((False, None), (True, False)),
)
def test_false_and_missing_no_change_proof_remain_false(
    include_no_change: bool,
    no_change_value: bool | None,
) -> None:
    source = _measured_source(
        latest_full_pipeline_scenario="single-delta",
        latest_full_pipeline_ok=True,
        latest_full_pipeline_latency_p95_ms=500.0,
        latest_full_pipeline_rows_appended=1,
        latest_full_pipeline_changed_graph_rows=1,
    )
    if include_no_change:
        source["latest_full_pipeline_no_change_dominant"] = no_change_value

    ledger = ledger_owner.build_source_optimization_ledger(
        _single_source_inventory(source),
        generated_at="2026-08-16T12:00:00+00:00",
    )
    row = ledger["rows"][0]
    relation_row = ledger_owner.source_optimization_ledger_relation_rows(ledger)[0]

    assert row["optimization_status"] == "passing_full_pipeline_benchmark"
    assert row["latest_full_pipeline_no_change_dominant"] is False
    assert relation_row["latest_full_pipeline_no_change_dominant"] is False


def test_failing_full_pipeline_proof_and_stage_ties_keep_current_policy() -> None:
    failing = _measured_source(
        latest_full_pipeline_scenario="single-delta",
        latest_full_pipeline_ok=False,
        latest_full_pipeline_latency_p95_ms=1_250.0,
        latest_full_pipeline_rows_appended=1,
        latest_full_pipeline_changed_graph_rows=1,
    )
    ledger = ledger_owner.build_source_optimization_ledger(
        _single_source_inventory(failing),
        generated_at="2026-08-16T12:00:00+00:00",
    )
    assert ledger["rows"][0]["optimization_status"] == "failing_budget"
    assert ledger["rows"][0]["proof_status"] == "full_pipeline_benchmark_failed"

    tied_stage_source = _measured_source(
        source_name="code-structure",
        latest_stage_total_ms=1_200.0,
        latest_stage_admission_prepare_ms=500.0,
        latest_stage_import_ms=500.0,
        latest_stage_source_runner_ms=100.0,
    )
    tied_ledger = ledger_owner.build_source_optimization_ledger(
        _single_source_inventory(tied_stage_source),
        generated_at="2026-08-16T12:00:00+00:00",
    )
    tied_row = tied_ledger["rows"][0]
    assert tied_row["bottleneck_stage"] == "admission.prepare"
    assert tied_row["root_cause_class"] == "ingest_stage_overhead"


def test_generated_at_is_exact_when_supplied_and_utc_now_by_default() -> None:
    inventory = _single_source_inventory(_measured_source())
    fixed = "caller-controlled-value"
    fixed_ledger = ledger_owner.build_source_optimization_ledger(
        inventory,
        generated_at=fixed,
    )
    assert fixed_ledger["generated_at"] == fixed

    before = datetime.now(timezone.utc)
    default_ledger = ledger_owner.build_source_optimization_ledger(inventory)
    after = datetime.now(timezone.utc)
    generated_at = datetime.fromisoformat(default_ledger["generated_at"])
    assert generated_at.utcoffset() == timezone.utc.utcoffset(generated_at)
    assert before <= generated_at <= after


def test_empty_inventory_characterization_is_exact_and_nonmutating() -> None:
    inventory = {
        "schema": "okg.source_sync.microbench.inventory.v1",
        "deployment": "slice3-empty",
        "generated_at": "2026-08-16T11:59:00+00:00",
        "source_count": 0,
        "enricher_count": 0,
        "live_state": {},
        "coverage_counts": {},
        "sources": [],
        "enrichers": [],
    }
    inventory_before = deepcopy(inventory)

    ledger = ledger_owner.build_source_optimization_ledger(
        inventory,
        generated_at="2026-08-16T12:00:00+00:00",
    )
    ledger_before_relation_rows = deepcopy(ledger)
    relation_rows = ledger_owner.source_optimization_ledger_relation_rows(ledger)

    assert inventory == inventory_before
    assert ledger == ledger_before_relation_rows
    assert ledger == {
        "schema": "okg.source_sync.microbench.optimization_ledger.v1",
        "deployment": "slice3-empty",
        "generated_at": "2026-08-16T12:00:00+00:00",
        "source_count": 0,
        "enricher_count": 0,
        "row_count": 0,
        "latency_budget_ms": 1000.0,
        "inventory_schema": "okg.source_sync.microbench.inventory.v1",
        "inventory_generated_at": "2026-08-16T11:59:00+00:00",
        "live_state": {},
        "coverage_counts": {},
        "optimization_status_counts": {},
        "optimization_wave_counts": {},
        "rows": [],
    }
    assert ledger_owner.MICROBENCH_OPTIMIZATION_LEDGER_ROWS_SCHEMA == (
        "okg.source_sync.microbench.optimization_ledger.rows.v1"
    )
    assert relation_rows == []
    assert ledger_owner.rank_source_optimization_rows([]) == []


def test_custom_latency_budget_propagates_and_classifies_against_threshold() -> None:
    inventory = _single_source_inventory(
        _measured_source(
            latest_source_runner_ms=750.0,
            latest_stage_total_ms=750.0,
        )
    )
    inventory_before = deepcopy(inventory)

    ledger = ledger_owner.build_source_optimization_ledger(
        inventory,
        latency_budget_ms=500.0,
        generated_at="2026-08-16T12:00:00+00:00",
    )
    row = ledger["rows"][0]
    relation_row = ledger_owner.source_optimization_ledger_relation_rows(ledger)[0]

    assert inventory == inventory_before
    assert ledger["latency_budget_ms"] == 500.0
    assert row["latency_budget_ms"] == 500.0
    assert relation_row["latency_budget_ms"] == 500.0
    assert row["latency_status"] == "over_budget"
    assert row["optimization_status"] == "failing_budget"
    assert row["proof_status"] == "latest_runner_sample_only"


def test_facade_exports_are_owner_objects_with_compatible_signatures() -> None:
    for name in MOVED_SYMBOLS:
        assert getattr(ledger_facade, name) is getattr(ledger_owner, name), name

    assert tuple(ledger_facade.__all__) == EXPECTED_FACADE_ALL
    assert "rank_source_optimization_rows" not in ledger_facade.__all__
    assert callable(ledger_facade.rank_source_optimization_rows)

    build_signature = inspect.signature(
        ledger_owner.build_source_optimization_ledger,
    )
    assert tuple(build_signature.parameters) == (
        "inventory",
        "latency_budget_ms",
        "generated_at",
    )
    assert build_signature.parameters["inventory"].kind is (
        inspect.Parameter.POSITIONAL_OR_KEYWORD
    )
    assert build_signature.parameters["latency_budget_ms"].kind is (
        inspect.Parameter.KEYWORD_ONLY
    )
    assert build_signature.parameters["latency_budget_ms"].default == (
        ledger_owner.SUBSECOND_LATENCY_BUDGET_MS
    )
    assert build_signature.parameters["generated_at"].kind is (
        inspect.Parameter.KEYWORD_ONLY
    )
    assert build_signature.parameters["generated_at"].default is None
    assert inspect.signature(ledger_facade.build_source_optimization_ledger) == (
        build_signature
    )
    assert inspect.signature(ledger_facade.rank_source_optimization_rows) == (
        inspect.signature(ledger_owner.rank_source_optimization_rows)
    )
    assert inspect.signature(
        ledger_facade.source_optimization_ledger_relation_rows,
    ) == inspect.signature(ledger_owner.source_optimization_ledger_relation_rows)

    with pytest.raises(AttributeError):
        ledger_owner.build_source_optimization_ledger(None)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        ledger_owner.rank_source_optimization_rows(None)  # type: ignore[arg-type]
    with pytest.raises(AttributeError):
        ledger_owner.source_optimization_ledger_relation_rows(
            None,  # type: ignore[arg-type]
        )


def test_installed_cli_ledger_executes_owner(tmp_path: Path) -> None:
    deployment_dir = _write_cli_deployment(tmp_path)
    entrypoint = shutil.which("okg")
    assert entrypoint is not None, "the installed okg entry point must be on PATH"
    env = os.environ.copy()
    env["OKG_DEPLOYMENTS_DIR"] = str(deployment_dir.parent)
    env.pop("OKG_DSN", None)
    owner_call_marker = _install_subprocess_owner_profiler(tmp_path, env)
    assert not owner_call_marker.exists()

    result = subprocess.run(
        [
            entrypoint,
            "--json",
            "sync",
            "microbench",
            "ledger",
            "--deployment",
            "slice3-cli",
            "--no-live",
        ],
        check=False,
        capture_output=True,
        env=env,
        text=True,
        timeout=30,
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["schema"] == ledger_owner.MICROBENCH_OPTIMIZATION_LEDGER_SCHEMA
    assert payload["row_count"] == 1
    assert payload["rows"][0]["name"] == "docs"
    assert payload["rows"][0]["optimization_status"] == "needs_benchmark"
    assert owner_call_marker.read_text(encoding="utf-8") == (
        "okg.substrate.source_sync_optimization_ledger:"
        "build_source_optimization_ledger:"
        f"{Path(ledger_owner.__file__).resolve()}\n"
    )


def test_deployment_readiness_composition_executes_owner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry_path = tmp_path / "source_registry.yaml"
    registry_path.write_text("sources: {}\n", encoding="utf-8")
    manifest = {
        "runtime": {"enabled": False},
        "readiness": {"source_optimization": {"enforcement": "block"}},
    }
    inventory = _single_source_inventory(
        _measured_source(
            latest_source_runner_ms=2_500.0,
            latest_stage_total_ms=2_500.0,
        )
    )
    acquisition_calls: list[dict[str, Any]] = []

    def _inventory_acquisition(**kwargs: Any) -> dict[str, Any]:
        acquisition_calls.append(kwargs)
        return deepcopy(inventory)

    monkeypatch.setattr(
        deployment_readiness._dep,
        "_ready_load_yaml_mapping",
        lambda *_args, **_kwargs: manifest,
    )
    monkeypatch.setattr(
        deployment_readiness._dep,
        "_ready_rollout_source_registry",
        lambda **_kwargs: registry_path,
    )
    monkeypatch.setattr(
        deployment_readiness._dep,
        "_ready_runtime_source_sync",
        lambda *_args, **_kwargs: ({}, []),
    )
    registry_module = importlib.import_module("okg.substrate.sources.registry")
    monkeypatch.setattr(registry_module, "load_source_registry", lambda _path: {})
    monkeypatch.setattr(
        ledger_facade,
        "build_microbenchmark_inventory",
        _inventory_acquisition,
    )

    with _record_owner_builder_calls() as owner_builder_calls:
        result = deployment_readiness._ready_source_optimization_status(
            deployment="slice3-readiness",
            deployment_dir=tmp_path,
            dsn="postgres://unused",
            max_producer_age_seconds=900,
        )

    assert owner_builder_calls == [
        (
            "okg.substrate.source_sync_optimization_ledger",
            str(Path(ledger_owner.__file__).resolve()),
        )
    ]
    assert len(acquisition_calls) == 1
    assert acquisition_calls[0]["include_live"] is True
    assert result["schema"] == "okg.source_optimization.readiness.v1"
    assert result["deployment"] == "slice3-test"
    assert result["readiness_required_count"] == 1
    assert result["status"] == "blocked"
    assert result["violations"][0]["source_name"] == "agent-sessions"


def test_okg_workspace_view_composition_executes_owner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifact = _characterization()
    view_module = _load_workspace_ledger_view(monkeypatch)
    written_rows: list[list[dict[str, Any]]] = []

    monkeypatch.setattr(
        view_module,
        "_scoped_relation_rows",
        lambda _conn, *, generation_id: None,
    )
    monkeypatch.setattr(
        view_module,
        "_inventory_for_view",
        lambda _conn: deepcopy(artifact["inventory"]),
    )
    monkeypatch.setattr(
        view_module,
        "_write_ledger_temp_table",
        lambda _conn, rows: written_rows.append(deepcopy(rows)),
    )

    with _record_owner_builder_calls() as owner_builder_calls:
        ledger = view_module._prepare_ledger_table(object(), generation_id=42)

    assert owner_builder_calls == [
        (
            "okg.substrate.source_sync_optimization_ledger",
            str(Path(ledger_owner.__file__).resolve()),
        )
    ]
    assert ledger["schema"] == ledger_owner.MICROBENCH_OPTIMIZATION_LEDGER_SCHEMA
    assert ledger["row_count"] == artifact["expected"]["row_count"]
    assert [row["name"] for row in ledger["rows"]] == (
        artifact["expected"]["ordered_names"]
    )
    assert len(written_rows) == 1
    assert written_rows[0] == (
        ledger_owner.source_optimization_ledger_relation_rows(ledger)
    )


def _load_workspace_ledger_view(
    monkeypatch: pytest.MonkeyPatch,
) -> ModuleType:
    package_name = "_slice3_okg_workspace_views"
    package = ModuleType(package_name)
    package.__path__ = [  # type: ignore[attr-defined]
        str(REPO_ROOT / "deployments" / "okg-workspace" / "views"),
    ]
    monkeypatch.setitem(sys.modules, package_name, package)
    return importlib.import_module(f"{package_name}.source_optimization_ledger")


def _write_cli_deployment(tmp_path: Path) -> Path:
    deployment_dir = tmp_path / "deployments" / "slice3-cli"
    deployment_dir.mkdir(parents=True)
    (deployment_dir / "schemas").mkdir()
    (deployment_dir / "docs").mkdir()
    (deployment_dir / "docs" / "guide.md").write_text(
        "# Slice 3\n",
        encoding="utf-8",
    )
    fixture_path = deployment_dir / "fixture.json"
    fixture_path.write_text('{"nodes": [], "edges": []}\n', encoding="utf-8")
    (deployment_dir / "deployment.yaml").write_text(
        "name: slice3-cli\n"
        "description: Slice 3 public-surface fixture\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n"
        "modules: []\n"
        "runtime:\n"
        "  enabled: true\n"
        "  source_sync:\n"
        "    watch:\n"
        "      enabled: true\n"
        "    poll:\n"
        "      enabled: false\n"
        "    webhook:\n"
        "      enabled: false\n",
        encoding="utf-8",
    )
    (deployment_dir / "source_registry.yaml").write_text(
        "sources:\n"
        "  docs:\n"
        "    module: okg.substrate.library.sources.bootstrap_fixture\n"
        "    class: BootstrapFixtureSource\n"
        "    ownership_id: slice3-cli.docs\n"
        "    params:\n"
        f"      fixture_path: {fixture_path}\n"
        "    source_class: discovery_crawl\n"
        "    record_identity_kind: scoped_locator\n"
        "    record_identity_fields: [path]\n"
        "    source_revision_kind: content_hash\n"
        "    deletion_semantics: missing_from_completed_scope\n"
        "    publication_mode: published_generation\n"
        "    sync:\n"
        "      watch:\n"
        f"        roots: [{deployment_dir}]\n"
        "        include_globs: ['docs/**/*.md']\n"
        "      triggers: [fs_event, manual, reconcile]\n"
        "      default_event_mode: scope_incomplete\n"
        "      reconcile_mode: scope_complete\n"
        "      scope:\n"
        "        accepts: [path]\n"
        "        unsupported_fallback: scope_complete\n",
        encoding="utf-8",
    )
    return deployment_dir
