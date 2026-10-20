"""req.abx.skeleton: manifest identity, resume refusal, resume skip."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from okg.bench.ab.cli import main
from okg.bench.ab.errors import ManifestMismatch
from okg.bench.ab.manifest import (
    RunManifest,
    check_resume,
    load_manifest,
    prompt_digest,
)


def _manifest(**over) -> RunManifest:
    base = dict(
        schema_version="okg-bench-ab.v1",
        run_id="r1",
        created_at="2026-07-27T00:00:00Z",
        benchmark="swebench-verified",
        arm="echo",
        model="sonnet",
        claude_cli_version=None,
        framework_git_sha=None,
        dataset_ref={"dataset": "d", "revision": "abc"},
        subset=None,
        pilot=True,
        limit=2,
        prompt_digest="p1",
        ordering={"mode": "pilot", "instance_ids": ["a", "b"]},
        budgets={"task_timeout_s": 1800.0, "permission_mode": "bypassPermissions"},
        native_memory_mode=None,
        judge=None,
        corpus=None,
        argv=[],
    )
    base.update(over)
    return RunManifest(**base)


def test_prompt_digest_is_order_sensitive():
    assert prompt_digest(["a", "b"]) != prompt_digest(["b", "a"])


def test_check_resume_accepts_identical_identity():
    check_resume(_manifest(), _manifest(run_id="r2", created_at="later"))


@pytest.mark.parametrize(
    "field,value",
    [
        ("arm", "fresh"),
        ("model", "opus"),
        ("dataset_ref", {"dataset": "d", "revision": "OTHER"}),
        ("prompt_digest", "p2"),
        ("limit", 3),
        # Review F2: corpus and judge are run identity — a resume against a
        # different corpus deployment or judge model must refuse.
        ("corpus", {"deployment_name": "other", "generation_id": "g2"}),
        ("judge", {"judge_model": "different/model"}),
    ],
)
def test_check_resume_refuses_identity_drift(field, value):
    with pytest.raises(ManifestMismatch) as err:
        check_resume(_manifest(), _manifest(**{field: value}))
    assert field in str(err.value)


def test_resume_skips_recorded_tasks_and_refuses_drift(
    tmp_path: Path, swebench_fixture: Path
):
    roots = [
        "--data-root", str(tmp_path / "data"),
        "--runs-root", str(tmp_path / "runs"),
    ]
    args = [
        "run", "--benchmark", "swebench-verified", "--arm", "echo",
        "--pilot", "--limit", "2", "--no-workdir",
        "--offline-fixture", str(swebench_fixture),
    ]
    assert main(roots + args) == 0
    run_dir = next((tmp_path / "runs").iterdir())
    first_mtimes = {
        p.name: p.stat().st_mtime_ns for p in (run_dir / "tasks").glob("*.json")
    }
    # Resume: identical invocation, records untouched (skipped, not re-run).
    assert main(roots + args + ["--resume", run_dir.name]) == 0
    second_mtimes = {
        p.name: p.stat().st_mtime_ns for p in (run_dir / "tasks").glob("*.json")
    }
    assert first_mtimes == second_mtimes
    # Drifted invocation refuses.
    drifted = [
        "run", "--benchmark", "swebench-verified", "--arm", "echo",
        "--pilot", "--limit", "1", "--no-workdir",
        "--offline-fixture", str(swebench_fixture),
        "--resume", run_dir.name,
    ]
    assert main(roots + drifted) == 2
    manifest = load_manifest(run_dir)
    assert manifest.limit == 2  # untouched


def test_manifest_records_reproducibility_fields(tmp_path, swebench_fixture):
    roots = [
        "--data-root", str(tmp_path / "data"),
        "--runs-root", str(tmp_path / "runs"),
    ]
    assert main(roots + [
        "run", "--benchmark", "swebench-verified", "--arm", "echo",
        "--pilot", "--limit", "1", "--no-workdir",
        "--offline-fixture", str(swebench_fixture),
    ]) == 0
    run_dir = next((tmp_path / "runs").iterdir())
    m = json.loads((run_dir / "manifest.json").read_text())
    for field in ("model", "arm", "benchmark", "dataset_ref", "prompt_digest",
                  "budgets", "ordering", "argv", "schema_version"):
        assert m[field] not in (None, "", [], {}), field
