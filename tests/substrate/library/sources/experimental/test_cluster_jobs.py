"""Tests for ClusterJobsSource (operator-pulled mode)."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

import pytest
import yaml

from okg.substrate.library.sources.base import NodeFact
from okg.substrate.library.sources.experimental import (
    ClusterJobsSource,
    ClusterPullModeError,
)


def _emit_nodes(src: ClusterJobsSource) -> tuple[list[NodeFact], dict]:
    run = src.run("run-1", mode="release_new")
    nodes = [f for f in run.facts if isinstance(f, NodeFact)]
    health = run.health.as_dict() if run.health is not None else {}
    return nodes, health


# ---------- happy paths ----------


def test_sacct_json_list_emits_compute_job(tmp_path: Path) -> None:
    """Slurm-style sacct -j --json output."""
    cache = tmp_path / "cluster01"
    cache.mkdir()
    (cache / "sacct.json").write_text(json.dumps([
        {
            "JobID": "1287",
            "Submit": "2026-05-15T10:00:00Z",
            "Start": "2026-05-15T10:01:00Z",
            "End": "2026-05-15T10:15:00Z",
            "Elapsed": 840,
            "ExitCode": 0,
        },
        {
            "JobID": "1288",
            "Submit": "2026-05-15T11:00:00Z",
            "End": "2026-05-15T11:30:00Z",
            "Elapsed": 1800,
            "ExitCode": 1,
        },
    ]))

    src = ClusterJobsSource(
        host="cluster01",
        cache_dir=tmp_path,
        job_log_glob="*.json",
    )
    nodes, health = _emit_nodes(src)
    assert len(nodes) == 2
    ids = sorted(n.node_id for n in nodes)
    assert ids == ["job:cluster01:1287", "job:cluster01:1288"]
    for n in nodes:
        assert n.subtype == "compute_job"
        assert n.attrs["cluster"] == "cluster01"
    assert health["status"] == "ok"
    assert health["record_count"] == 2


def test_yaml_jobs_key_emits(tmp_path: Path) -> None:
    cache = tmp_path / "gridlogin"
    cache.mkdir()
    (cache / "manual.yaml").write_text(yaml.safe_dump({
        "jobs": [
            {"job_id": "abc-1", "exit_status": 0},
            {"job_id": "abc-2", "exit_status": 0},
        ],
    }))
    src = ClusterJobsSource(
        host="gridlogin",
        cache_dir=tmp_path,
        job_log_glob="*.yaml",
    )
    nodes, _ = _emit_nodes(src)
    assert {n.node_id for n in nodes} == {
        "job:gridlogin:abc-1",
        "job:gridlogin:abc-2",
    }


def test_flat_cache_layout(tmp_path: Path) -> None:
    """Cache without a per-host subdirectory works for
    single-cluster deployments."""
    (tmp_path / "dump.json").write_text(json.dumps([
        {"job_id": "42", "cluster": "local"},
    ]))
    src = ClusterJobsSource(
        host="local",
        cache_dir=tmp_path,
        job_log_glob="*.json",
    )
    nodes, _ = _emit_nodes(src)
    assert len(nodes) == 1
    assert nodes[0].node_id == "job:local:42"


def test_attrs_round_trip(tmp_path: Path) -> None:
    cache = tmp_path / "cluster01"
    cache.mkdir()
    (cache / "j.json").write_text(json.dumps([
        {
            "JobID": "9999",
            "Submit": "2026-05-15T08:00:00Z",
            "Start": "2026-05-15T08:01:00Z",
            "End": "2026-05-15T08:30:00Z",
            "Elapsed": 1740,
            "ExitCode": 0,
            "gpu_hours": 0.5,
            "submit_script_ref": "slurm/runs/k-2026-05-15.sbatch",
            "User": "operator",
        },
    ]))
    src = ClusterJobsSource(host="cluster01", cache_dir=tmp_path)
    nodes, _ = _emit_nodes(src)
    n = nodes[0]
    assert n.attrs["submitted_at"] == "2026-05-15T08:00:00Z"
    assert n.attrs["started_at"] == "2026-05-15T08:01:00Z"
    assert n.attrs["finished_at"] == "2026-05-15T08:30:00Z"
    assert n.attrs["wall_clock_seconds"] == 1740
    assert n.attrs["exit_status"] == 0
    assert n.attrs["gpu_hours"] == 0.5
    assert n.attrs["submit_script_ref"] == "slurm/runs/k-2026-05-15.sbatch"
    assert n.attrs["user"] == "operator"


# ---------- failure / stale paths ----------


def test_substrate_pulled_mode_raises(tmp_path: Path) -> None:
    src = ClusterJobsSource(
        host="cluster01",
        cache_dir=tmp_path,
        pull_mode="substrate-pulled",
    )
    with pytest.raises(ClusterPullModeError) as exc_info:
        list(src.run("r", mode="release_new").facts)
    assert "operator-pulled" in (exc_info.value.fix_hint or "")


def test_unknown_pull_mode_raises_at_init(tmp_path: Path) -> None:
    with pytest.raises(ClusterPullModeError):
        ClusterJobsSource(
            host="cluster01",
            cache_dir=tmp_path,
            pull_mode="nonsense",
        )


def test_missing_cache_dir_is_loud_in_health(tmp_path: Path) -> None:
    src = ClusterJobsSource(
        host="cluster01",
        cache_dir=tmp_path / "does-not-exist",
    )
    nodes, health = _emit_nodes(src)
    assert nodes == []
    assert health["status"] == "cache_missing"
    assert "does-not-exist" in health["cache_path"]


def test_stale_cache_warning(tmp_path: Path) -> None:
    cache = tmp_path / "cluster01"
    cache.mkdir()
    (cache / "old.json").write_text(json.dumps([
        {"job_id": "stale-1"},
    ]))
    # Set mtime far in the past.
    long_ago = time.time() - (3 * 86400)
    os.utime(cache / "old.json", (long_ago, long_ago))

    src = ClusterJobsSource(
        host="cluster01",
        cache_dir=tmp_path,
        staleness_warn_seconds=86400,
    )
    nodes, health = _emit_nodes(src)
    assert len(nodes) == 1
    assert health["status"] == "skipped_optional"
    assert health["reason"] is not None
    assert "stale" in health["reason"]


def test_empty_cache_reports_reason(tmp_path: Path) -> None:
    cache = tmp_path / "cluster01"
    cache.mkdir()
    src = ClusterJobsSource(host="cluster01", cache_dir=tmp_path)
    nodes, health = _emit_nodes(src)
    assert nodes == []
    assert health["reason"] is not None


def test_malformed_log_silently_skipped(tmp_path: Path) -> None:
    """An unparseable file is skipped; well-formed peers still emit."""
    cache = tmp_path / "cluster01"
    cache.mkdir()
    (cache / "broken.json").write_text("{not json")
    (cache / "good.json").write_text(json.dumps([
        {"job_id": "g-1"},
    ]))
    src = ClusterJobsSource(host="cluster01", cache_dir=tmp_path)
    nodes, _ = _emit_nodes(src)
    assert {n.node_id for n in nodes} == {"job:cluster01:g-1"}


def test_no_hostnames_hardcoded_in_module_code() -> None:
    """Sanity: cluster_jobs.py source SHALL NOT contain memex-specific
    hostnames; cluster slugs are operator config only."""
    from okg.substrate.library.sources.experimental import cluster_jobs
    src_text = Path(cluster_jobs.__file__).read_text()
    # Cluster slugs may appear in docstrings as examples, but no fully
    # qualified host under the institutions' domains may be hardcoded.
    # Checking the domain suffix covers every host there, not one name.
    assert ".mit.edu" not in src_text
    assert ".cern.ch" not in src_text
