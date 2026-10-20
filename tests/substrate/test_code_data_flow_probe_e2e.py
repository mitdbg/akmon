"""End-to-end #323 proof on the worst offender: code-data-flow under churn.

This is the substrate behavior the churn microbench measures, proven directly:
a no-change cycle — even after mtime churn — skips the adapter's FileWalk + AST
extraction entirely (the regime that regressed the per-file fix to ~106s). A
real content edit still reads; the probe-force-off knob makes it read regardless.
Uses a migrated ephemeral test DB + a real temp git repo.
"""
from __future__ import annotations

import os
import subprocess

import pytest

from okg.substrate.ingest.runner import SourceRunner
from okg.substrate.library.sources.code_graph_dataflow import CodeDataFlowSource


pytestmark = pytest.mark.postgres


def _git(repo, *args):
    subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True)


class _CountingCodeDataFlow(CodeDataFlowSource):
    """CodeDataFlowSource that counts how often its reader actually runs."""

    def __post_init__(self):
        super().__post_init__()
        self.run_count = 0

    def run(self, run_id, *, mode="cursor", cursor=None, sync_scope=None):
        self.run_count += 1
        return super().run(run_id, mode=mode, cursor=cursor, sync_scope=sync_scope)


@pytest.fixture
def repo(tmp_path):
    r = tmp_path / "repo"
    r.mkdir()
    _git(r, "init", "-q")
    _git(r, "config", "user.email", "t@example.com")
    _git(r, "config", "user.name", "Tester")
    # os.getenv(...) produces a config_key fact, so the first read emits.
    (r / "app.py").write_text("import os\nDB = os.getenv('DB_URL')\n")
    _git(r, "add", "-A")
    _git(r, "commit", "-q", "-m", "init")
    return r


@pytest.fixture
def runner(okg_migrated, request):
    # Unique source per test: the watermark persists in the shared ephemeral DB,
    # so a shared name would let one test's stamped token short-circuit the
    # next test's first run.
    return SourceRunner(
        okg_migrated,
        source_name=f"cdf-e2e-{request.node.name}",
        deployment_name=None,
        write_chronos_graph=False,
        write_fact_log=True,
    )


def test_no_change_under_mtime_churn_skips_extraction(runner, repo):
    adapter = _CountingCodeDataFlow(repo="demo", repo_root=str(repo))

    # First cursor run: no prior token => reader runs and emits.
    r1 = runner.run(adapter, mode="cursor")
    assert adapter.run_count == 1
    assert r1.nodes_appended > 0  # config_key from os.getenv('DB_URL')

    # mtime churn: touch every tracked file (content identical). The per-file
    # mtime fast-path would re-read all of these; the git scope probe does not.
    far_future = 2_000_000_000
    os.utime(repo / "app.py", (far_future, far_future))

    # Second cursor run: probe is unchanged despite the churn => the reader
    # (FileWalk + AST extraction) is never invoked.
    r2 = runner.run(adapter, mode="cursor")
    assert adapter.run_count == 1, "no-change-under-churn must skip the reader"
    assert r2.nodes_appended == 0
    assert r2.completed_scope is False


def test_content_edit_invokes_reader(runner, repo):
    adapter = _CountingCodeDataFlow(repo="demo", repo_root=str(repo))
    runner.run(adapter, mode="cursor")
    assert adapter.run_count == 1

    # A real content change must bust the probe and re-read.
    (repo / "app.py").write_text("import os\nDB = os.getenv('DB_URL')\nQ = os.getenv('QUEUE')\n")
    runner.run(adapter, mode="cursor")
    assert adapter.run_count == 2


def test_probe_force_off_knob_forces_a_read(runner, repo, monkeypatch):
    adapter = _CountingCodeDataFlow(repo="demo", repo_root=str(repo))
    runner.run(adapter, mode="cursor")
    assert adapter.run_count == 1

    # With the gate disabled, an unchanged scope still reads (the microbench's
    # probe-forced-off discrimination arm => files_scanned > 0).
    monkeypatch.setenv("OKG_DISABLE_CHANGE_PROBE_GATE", "1")
    runner.run(adapter, mode="cursor")
    assert adapter.run_count == 2, "force-off knob must make the reader run"
